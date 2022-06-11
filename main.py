import datetime
import os
import re
import sys

import pandas as pd
import jaydebeapi


# 1. Загрузка в STG

# 1.1 Очистка STG
 
def truncate_stg(cursor):
    list_stg_autoclean = ['transactions', 'pssprt_blcklst', 'terminals', 'cards', 'accounts', 'clients']
    for table in list_stg_autoclean:
        stg_clean = 'truncate table de3at.lugm_stg_{table}'
        stg_clean = stg_clean.format(table=table)
        cursor.execute(stg_clean)
    
    list_stg_del_autoclean = ['terminals', 'cards', 'accounts', 'clients'] 
    for table in list_stg_del_autoclean:
        stg_del_clean = 'truncate table de3at.lugm_stg_{table}_del'
        stg_del_clean = stg_del_clean.format(table=table)
        cursor.execute(stg_del_clean)

# 1.2 Загрузка в STG файлов excel и txt

PATH_TO_XLSX_CSV = '/home/de3at/lugm'

# Словари со столбцами соответствующих таблиц, для автоматизации формаирования sql-скриптов
dict_autofill = {'cards':['card_num', 'account'], 
                'accounts':['account', 'valid_to', 'client'],
                'clients':['client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 
                        'passport_num', 'passport_valid_to', 'phone'],
                'terminals':['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address']}

dict_fact_autofill = {'transactions':['trans_id', 'trans_date', 'card_num', 'oper_type', 'amt', 'oper_result', 'terminal'], 
                    'pssprt_blcklst':['passport_num', 'entry_dt']}

def transactions_to_stg(cursor):
    '''Для инкрементальной загрузки при наличии в каталоге нескольких файлов с различными датами
    функция находит файл transactions с минимальной датой, 
    передает его содержимое в stg и перемещает файл в каталог archive ''' 
    
    s = '''
        insert into de3at.lugm_stg_transactions(trans_id, trans_date, amt, card_num , oper_type, oper_result, terminal) values
        (?,to_date(?,'YYYY-MM-DD HH24:MI:SS') , ?, ?, ?, ?, ?)
    '''
    dict_date_of_files ={}
    for f in os.listdir(PATH_TO_XLSX_CSV):
        if f.startswith('transactions'):
            dict_date_of_files[datetime.datetime.strptime(re.findall('[0-9]+', os.path.basename(f))[0],'%d%m%Y').date()] = \
            os.path.basename(f)
    f_min_date = dict_date_of_files[min(dict_date_of_files, key=dict_date_of_files.get)]
    df = pd.read_csv(f_min_date, sep=';')
    df['amount'] = df['amount'].str.replace(',', '.').astype('float')
    tup_df = [tuple(y for y in x) for x in df.values]
    cursor.executemany(s, tup_df)
    os.replace(f_min_date, "archive/"+f_min_date+".backup")

def terminals_to_stg(cursor):
    s = '''
    insert into de3at.lugm_stg_terminals(terminal_id, terminal_type, terminal_city, terminal_address, update_dt) values
    (?, ?, ?, ?, to_date(?, 'YYYY-MM-DD'))
    '''

    dict_date_of_files ={}
    for f in os.listdir(PATH_TO_XLSX_CSV):
        if f.startswith('terminals'):
            dict_date_of_files[datetime.datetime.strptime(re.findall('[0-9]+', os.path.basename(f))[0],'%d%m%Y').date()] = \
            os.path.basename(f)
    f_min_date = dict_date_of_files[min(dict_date_of_files, key=dict_date_of_files.get)]
    df = pd.read_excel(f_min_date)
    df['update_dt'] = min(dict_date_of_files, key=dict_date_of_files.get).strftime("%Y-%m-%d")
    cursor.executemany(s, [tuple(y for y in x) for x in df.values])
    cursor.execute('insert into de3at.lugm_stg_terminals_del (terminal_id) select terminal_id from de3at.lugm_stg_terminals')
    os.replace(f_min_date, "archive/"+f_min_date+".backup")


def passport_blacklist_to_stg(cursor):
    s = '''
    insert into de3at.lugm_stg_pssprt_blcklst (entry_dt, passport_num) values
    (to_date(?, 'YYYY-MM-DD'), ?)
    '''
    dict_date_of_files ={}
    for f in os.listdir(PATH_TO_XLSX_CSV):
        if f.startswith('passport_blacklist'):
            dict_date_of_files[datetime.datetime.strptime(re.findall('[0-9]+', os.path.basename(f))[0],'%d%m%Y').date()] = \
            os.path.basename(f)
    f_min_date = dict_date_of_files[min(dict_date_of_files, key=dict_date_of_files.get)]
    df = pd.read_excel(f_min_date)
    df['date'] = df['date'].astype('str')
    tup_df = [tuple(y for y in x) for x in df.values]
    cursor.executemany(s, tup_df)
    os.replace(f_min_date, "archive/"+f_min_date+".backup")

def db_to_stg(cursor):
    s = '''
    insert into de3at.lugm_stg_{table} ({fields}, update_dt)
    select {fields}, coalesce(update_dt, create_dt)
    from bank.{table}
    where coalesce(update_dt, create_dt) > ( 
        select last_update_dt
        from de3at.lugm_meta
        where schema_name = 'de3at' and table_name = '{table}')
    '''
    for table, fields in dict_autofill.items():
        if table not in 'terminals':
            sql = s.format(table=table, fields=', '.join(fields))
            cursor.execute(sql)

def db_to_stg_del(cursor):
    s = '''
    insert into de3at.lugm_stg_{table}_del ({fields[0]})
    select {fields[0]} from bank.{table}
    '''
    for table, fields in dict_autofill.items():
        if table != 'terminals':
            sql = s.format(table=table, fields=fields)
            cursor.execute(sql)

# 2. Выделение вставок и изменений (transform) вставка их приемник (load)

def insert_tgt_from_stg(cursor):
    s = '''
    insert into de3at.lugm_dwh_dim_{table}_his ({str_fields}, effective_from, effective_to, deleted_flg) 
    select
        {stg_fields}
        stg.update_dt,
        to_date( '31.12.9999', 'DD.MM.YYYY' ), 
        'N'
    from de3at.lugm_dwh_dim_{table}_his tgt
        inner join de3at.lugm_stg_{table} stg
            on ( stg.{fields[0]} = tgt.{fields[0]} and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where 1=0
    '''
    s_add = '''
    or stg.{field} <> tgt.{field} or ( stg.{field} is null and tgt.{field} is not null ) or ( stg.{field} is not null and tgt.{field} is null ) 
    '''
    for table, fields in dict_autofill.items():
        stg_fields =''
        for field in fields:
            f = 'stg.' + field + ', '
            stg_fields += f
        str_fields = ', '.join(fields)
        sql = s.format(table=table, str_fields = str_fields, stg_fields=stg_fields, fields=fields)
        for field in fields:
            sql += s_add.format(field=field)
        cursor.execute(sql)

def merge_tgt_from_stg(cursor):
    s_match = '''
    merge into de3at.lugm_dwh_dim_{table}_his tgt
    using de3at.lugm_stg_{table} stg
        on( stg.{fields[0]} = tgt.{fields[0]} and deleted_flg = 'N' )
    when matched then 
        update set tgt.effective_to = stg.update_dt - interval '1' second
        where 1=1
        and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
        and (1=0
    '''
    s_add = '''or stg.{field} <> tgt.{field} or ( stg.{field} is null and tgt.{field} is not null ) or ( stg.{field} is not null and tgt.{field} is null ) '''

    s_not_match = '''
        )
    when not matched then 
        insert ( 
        {str_fields},
        effective_from, effective_to, deleted_flg  ) 
        values ( 
        {stg_fields} stg.update_dt, to_date( '31.12.9999', 'DD.MM.YYYY' ), 'N' )
    '''
    for table, fields in dict_autofill.items():
        stg_fields =''
        for field in fields:
            f = 'stg.' + field + ', '
            stg_fields += f
        
        str_fields = ', '.join(fields)
        
        sql = s_match.format(table=table, str_fields = str_fields, stg_fields=stg_fields, fields=fields)
        for field in fields:
            sql += s_add.format(field=field)
        sql += s_not_match.format(str_fields = str_fields, stg_fields=stg_fields)
        cursor.execute(sql)

def insert_tgt_fact_from_stg(cursor):
    s = '''
    insert into de3at.lugm_dwh_fact_{table} ({str_fields})
    select
        {stg_fields}
    from de3at.lugm_stg_{table} stg
        left join de3at.lugm_dwh_fact_{table} tgt 
            on stg.{fields[0]} = tgt.{fields[0]}
    where ( tgt.{fields[0]} is null)
    '''

    for table, fields in dict_fact_autofill.items():
        str_fields = ', '.join(fields)
        stg_fields = ''
        for field in fields:
            f = 'stg.' + field + ', '
            stg_fields += f
        sql = s.format(table=table, str_fields = str_fields, stg_fields=stg_fields[:-2], fields=fields)
        cursor.execute(sql)

# 3. Обработка удалений.
def insert_tgt_from_stg_del(cursor):
    s = '''
    insert into de3at.lugm_dwh_dim_{table}_his ( {str_fields}, effective_from, effective_to, deleted_flg  ) 
        select
        {tgt_fields}
        current_date, 
        to_date( '31.12.9999', 'DD.MM.YYYY' ), 
        'Y'
    from de3at.lugm_dwh_dim_{table}_his tgt
        left join de3at.lugm_stg_{table}_del stg
            on  stg.{fields[0]} = tgt.{fields[0]} 
    where stg.{fields[0]} is null and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N'
    '''
    for table, fields in dict_autofill.items():
        tgt_fields =''
        for field in fields:
            f = 'tgt.' + field + ', '
            tgt_fields += f
        str_fields = ', '.join(fields)
        sql = s.format(table=table, str_fields = str_fields, tgt_fields=tgt_fields, fields=fields)
        cursor.execute(sql)

def update_tgt_from_stg_del(cursor):
    s = '''
    update de3at.lugm_dwh_dim_{table}_his tgt
    set effective_to = current_date - interval '1' second
    where tgt.{fields[0]} not in (select {fields[0]} from de3at.lugm_stg_{table}_del)
        and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
        and tgt.deleted_flg = 'N'
    '''
    for table, fields in dict_autofill.items():
        sql = s.format(table=table, fields=fields)
        cursor.execute(sql)

# 4. Обновление метаданных.

def update_meta(cursor):
    s = '''
    update lugm_meta
    set last_update_dt = (select coalesce(max(stg.update_dt), max(tgt.effective_from))
    from de3at.lugm_stg_{table} stg
            full join de3at.lugm_dwh_dim_{table}_his tgt using ({fields[0]}))
    where schema_name = 'de3at' and table_name = '{table}'
'''
    for table, fields in dict_autofill.items():
        sql = s.format(table=table, fields=fields)
        cursor.execute(sql)

# 5. Обновление отчета по мошенническим операциям

report_file = open("sql_scripts/report.sql", 'r')
query = report_file.read()
sql_update_report = query.split(';')[0]
sql_update_meta_report = query.split(';')[1]
report_file.close()

def update_report(cursor):
    cursor.execute(sql_update_report)

def update_meta_report(cursor):
    cursor.execute(sql_update_meta_report)


def get_conn():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de3at/bardthebowman@de-oracle.chronosavant.ru:1521/deoracle',
        ['de3at','bardthebowman'],
        '/home/de3at/ojdbc8.jar'
    )
    return conn

def run_db_cmds(cmd_list):
    conn = get_conn()
    cursor = conn.cursor()

    errors = []
    for cmd in cmd_list:
        try:
            cmd(cursor)
        except Exception as e:
            errors.append(str(e))
    return (len(errors) == 0, errors)


INCREMENT_COMMANDS = [
    truncate_stg,
    transactions_to_stg,
    terminals_to_stg,
    passport_blacklist_to_stg,
    db_to_stg,
    db_to_stg_del,
    insert_tgt_fact_from_stg,
    insert_tgt_from_stg,
    merge_tgt_from_stg,
    insert_tgt_from_stg_del,
    update_tgt_from_stg_del,
    update_meta
    ]

REPORT_COMMANDS = [
    update_report,
    update_meta_report
    ]


if __name__ == '__main__':
    cmd = sys.argv[1]

    if cmd == 'run_increment':
        is_ok, errors = run_db_cmds(INCREMENT_COMMANDS)
    elif cmd == 'make_report':
        is_ok, errors = run_db_cmds(REPORT_COMMANDS)
    else:
        raise NotImplementedError('Unknown command')

    if not is_ok:
        print(*errors, sep='\n')

