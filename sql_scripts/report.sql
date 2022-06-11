insert into DE3AT.LUGM_REP_FRAUD (event_dt, passport_num, fio, phone, event_type, report_dt)
select 
    rep.event_dt, 
    rep.passport_num, 
    rep.fio, 
    rep.phone, 
    rep.event_type, 
    rep.report_dt 
from 
(with full_table as (
    select
        trans_id,
        amt,
        oper_result,
        terminal_city,
        trans_date,
        cln.passport_num as passport,
        last_name,
        first_name,
        patronymic,
        phone,
        psbl.passport_num as blocked_passport,
        entry_dt,
        valid_to,
        passport_valid_to,
        cast(lead(trans_date) over (partition by trn.card_num order by trans_date) as timestamp) - cast(trans_date as timestamp) as time_btwn_trns,
        lead(terminal_city) over (partition by trn.card_num order by trans_date) as city_btwn_trns,
        row_number() OVER (PARTITION BY client_id ORDER BY trans_date) AS num_trns,
        cast(trans_date as timestamp) - cast(lag(trans_date,3) over (partition by trn.card_num order by trans_date) as timestamp) as time_btwn_three_trns,
        case
            when oper_result = 'SUCCESS' and
                 lag(oper_result,2) over (partition by trn.card_num order by trans_date) = 'REJECT' and
                 lag(oper_result,1) over (partition by trn.card_num order by trans_date) = 'REJECT' and
                 lag(amt,1) over (partition by trn.card_num order by trans_date) > amt and
                 lag(amt,2) over (partition by trn.card_num order by trans_date) > lag(amt,1) over (partition by trn.card_num order by trans_date)
            then 1
            else 0
        end as amount_guessing 
    from 
        DE3AT.LUGM_DWH_FACT_TRANSACTIONS trn
            join DE3AT.LUGM_DWH_DIM_CARDS_HIS  crd on trn.card_num = rtrim(crd.card_num)
            join  DE3AT.LUGM_DWH_DIM_ACCOUNTS_HIS acc on crd.account = acc.account
            join  DE3AT.LUGM_DWH_DIM_CLIENTS_HIS cln on acc.client = cln.client_id
            join  DE3AT.LUGM_DWH_DIM_TERMINALS_HIS trm on trn.terminal = trm.terminal_id
            left join DE3AT.LUGM_DWH_FACT_PSSPRT_BLCKLST psbl on cln.passport_num = psbl.passport_num
        where trunc(trn.trans_date, 'DD') = (select (max(trunc(trans_date, 'DD'))) from DE3AT.LUGM_DWH_FACT_TRANSACTIONS)
)

--Совершение операции при недействующем контракте
select
    trans_date as event_dt,
    passport as passport_num,
    last_name || ' ' || first_name || ' ' || patronymic as fio,
    phone as phone,
    'expired_contract' as event_type,
    current_date as report_dt
from full_table
where trans_date > valid_to and num_trns = 1

union all

----Совершение операции при заблокированном паспорте
select
    trans_date as event_dt,
    passport as passport_num,
    last_name || ' ' || first_name || ' ' || patronymic as fio,
    phone as phone,
    'blocked_passport' as event_type,
    current_date as report_dt
from full_table
where blocked_passport is not null and trans_date > entry_dt and num_trns = 1

union all

--Совершение операции при просроченном паспорте
select
    trans_date as event_dt,
    passport as passport_num,
    last_name || ' ' || first_name || ' ' || patronymic as fio,
    phone as phone,
    'expired_passport' as event_type,
    current_date as report_dt  
from full_table
where trans_date > passport_valid_to and num_trns = 1

union all

--Совершение операций в разных городах в течение одного часа.
select
    trans_date as event_dt,
    passport as passport_num,
    last_name || ' ' || first_name || ' ' || patronymic as fio,
    phone as phone,
    'transactions from different cities' as event_type,
    current_date as report_dt
from full_table
where time_btwn_trns < interval '1' hour and city_btwn_trns <> terminal_city

union all

--Попытка подбора суммы.
select
    trans_date as event_dt,
    passport as passport_num,
    last_name || ' ' || first_name || ' ' || patronymic as fio,
    phone as phone,
    'trying to guess the balance' as event_type,
    current_date as report_dt
from full_table
where time_btwn_three_trns < interval '20' minute and amount_guessing = 1) rep
where rep.event_dt > ( 
    select last_update_dt
    from de3at.lugm_meta_rep
    where schema_name = 'de3at' and table_name = 'rep_fraud');
    
    
update lugm_meta_rep
set last_update_dt = (select max(rep_fr.event_dt)
from DE3AT.LUGM_REP_FRAUD rep_fr)
where schema_name = 'de3at' and table_name = 'rep_fraud'