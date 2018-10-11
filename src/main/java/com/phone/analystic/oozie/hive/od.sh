#!/bin/bash

##### ./od.sh -d 2018-09-19

dt=''
#循环运行时所带的参数
until [ $# -eq 0 ]
do
if [ $1'x' = '-dx' ]
then
shift
dt=$1
fi
shift
done

month=
day=
#判断日期是否合法和正常
if [ ${#dt} = 10 ]
then
echo "dt:$dt"
else
dt=`date -d "1 days ago" "+%Y-%m-%d"`
fi


#计算month和day
month=`date -d "$dt" "+%m"`
day=`date -d "$dt" "+%d"`
echo "running date is:$dt,month is:$month,day is:$day"
echo "running hive SQL statment..."
#run hive hql

hive --database log_phone -e "
set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
with tmp as(
select
from_unixtime(cast(l.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
l.en as en,
if((case when l.en = 'e_crt' then count(distinct l.oid) end) is null,0,(case when l.en = 'e_crt' then count(distinct l.oid) end))as orders,
if((case when l.en = 'e_cs' then count(distinct l.oid) end) is null,0,(case when l.en = 'e_cs' then count(distinct l.oid) end))as success_orders,
if((case when l.en = 'e_cr' then count(distinct l.oid) end) is null,0,(case when l.en = 'e_cr' then count(distinct l.oid) end))as refund_orders
from phone l
where l.month = "${month}"
and l.day = "${day}"
and l.oid is not null
and l.oid <> 'null'
group by from_unixtime(cast(l.s_time/1000 as bigint),'yyyy-MM-dd'),pl,cut,pt,l.en
)
from(
select dt as dt1,pl as pl ,cut as cut,pt as pt,orders as orders,0 as success_orders,0 as refund_orders,dt from tmp where en = 'e_crt'
union all
select dt as dt1,pl as pl ,cut as cut,pt as pt,0 as orders,success_orders as success_orders,0 as refund_orders,dt from tmp where en = 'e_cs'
union all
select dt as dt1,pl as pl ,cut as cut,pt as pt,0 as orders,0 as success_orders,refund_orders as refund_orders,dt from tmp where en = 'e_cr'
) as tmp1
insert overwrite table stats_order_tmp1
select phone_date(dt1),phone_platform(pl),phone_currency(cut),phone_pay(pt),sum(orders),sum(success_orders),sum(refund_orders),dt1
group by dt1,pl,cut,pt
;
"


#run sqoop statment1
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root -m 1 \
--table stats_order --export-dir /hive/log_phone.db/stats_order_tmp1/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,success_orders,refund_orders,created' \
;

hive --database log_phone -e "
set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
from(
select
from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
p.pl as pl,
p.cut as cut,
p.pt as pt,
if((case when p.en = 'e_crt' then sum(p.cua) end) is null,0,(case when p.en = 'e_crt' then sum(p.cua) end))as orders_amount,
if((case when s.en = 'e_cs' then sum(p.cua) end) is null,0,(case when s.en = 'e_cs' then sum(p.cua) end))as success_orders_amount,
if((case when r.en = 'e_cr' then sum(p.cua) end) is null,0,(case when r.en = 'e_cr' then sum(p.cua) end))as refund_orders_amount
from phone p
left join phone s
on s.oid = p.oid and s.en = 'e_cs'
left join phone r
on r.oid = s.oid and r.en = 'e_cr'
where p.month = "${month}"
and p.day = "${day}"
and p.oid is not null
and p.oid <> 'null'
and p.en = 'e_crt'
group by from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),p.pl,p.cut,p.pt,p.en,s.en,r.en
) as tmp
insert overwrite table stats_order_tmp2
select phone_date(dt),phone_platform(pl),phone_currency(cut),phone_pay(pt),sum(orders_amount),sum(success_orders_amount),sum(refund_orders_amount),dt
group by dt,pl,cut,pt
;
"

#run sqoop statment
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root -m 1 \
--table stats_order --export-dir /hive/log_phone.db/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id,order_amount,revenue_amount,refund_amount,created' \
;

echo "the orders job is fininshed."