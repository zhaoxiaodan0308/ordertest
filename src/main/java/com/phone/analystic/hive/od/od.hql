1、创建维度类，并创建对应的udf方法
create function phone_pay as 'com.phone.analystic.hive.PaymentTypeDimensionUdf' using jar 'hdfs://hadoop01:9000/phone/udfjars/phone_analystic-1.0.jar';
create function phone_currency as 'com.phone.analystic.hive.CurrencyTypeDimensionUdf' using jar 'hdfs://hadoop01:9000/phone/udfjars/phone_analystic-1.0.jar';

2.创建表
  create table if not exists stats_order_tmp(
        `date_dimension_id` int,
        `platform_dimension_id` int,
        `currency_type_dimension_id` int,
        `payment_type_dimension_id` int,
        `ct` int,
        `created` string
  )
  ;

  create table if not exists stats_order_tmp1(
        `date_dimension_id` int,
        `platform_dimension_id` int,
        `currency_type_dimension_id` int,
        `payment_type_dimension_id` int,
        `orders` int,
        `success_orders` int,
        `refund_orders` int,
        `created` string
  )
  ;

  create table if not exists stats_order_tmp2(
        `date_dimension_id` int,
        `platform_dimension_id` int,
        `currency_type_dimension_id` int,
        `payment_type_dimension_id` int,
        `orders_amount` double,
        `success_orders_amount` double,
        `refund_orders_amount`  double,
        `created` string
  )
  ;

3、写指标
3、写指标

--总的订单数量：

from(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
count(distinct l.o_id) as ct
from phone l
where l.month = 9
and day = 24
and l.o_id is not null
and l.o_id <> 'null'
and l.en = 'e_crt'
group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,cut,pt
) as tmp
insert overwrite table stats_order_tmp
select phone_date(dt),phone_platform(pl),phone_currency(cut),phone_pay(pt),sum(ct),dt
group by dt,pl,cut,pt
;

sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root \
--table stats_order --export-dir /hive/log_phone.db/stats_order_tmp/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created' \
;

--成功支付的订单数量：

from(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
count(distinct l.o_id) as ct
from phone l
where l.month = 9
and day = 24
and l.o_id is not null
and l.o_id <> 'null'
and l.en = 'e_cs'
group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,cut,pt
) as tmp
insert overwrite table stats_order_tmp
select phone_date(dt),phone_platform(pl),phone_currency(cut),phone_pay(pt),sum(ct),dt
group by dt,pl,cut,pt
;

sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root \
--table stats_order --export-dir /hive/log_phone.db/stats_order_tmp/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id,success_orders,created' \
;

--退款成功的订单
from(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
count(distinct l.o_id) as ct
from phone l
where l.month = 8
and day = 18
and l.o_id is not null
and l.o_id <> 'null'
and l.en = 'e_cr'
group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,cut,pt
) as tmp
insert overwrite table stats_order_tmp
select phone_date(dt),phone_platform(pl),phone_currency(cut),phone_pay(pt),sum(ct),dt
group by dt,pl,cut,pt
;

sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root -m 1 \
--table stats_order --export-dir /hive/log_phone.db/stats_order_tmp/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_orders,created' \
;


########计算订单个数的写成一条语句如下：

set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
with tmp as(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
l.en as en,
if((case when l.en = 'e_crt' then count(distinct l.oid) end) is null,0,(case when l.en = 'e_crt' then count(distinct l.oid) end))as orders,
if((case when l.en = 'e_cs' then count(distinct l.oid) end) is null,0,(case when l.en = 'e_cs' then count(distinct l.oid) end))as success_orders,
if((case when l.en = 'e_cr' then count(distinct l.oid) end) is null,0,(case when l.en = 'e_cr' then count(distinct l.oid) end))as refund_orders
from phone l
where l.month = 9
and day = 24
and l.oid is not null
and l.oid <> 'null'
group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,cut,pt,l.en
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


sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root -m 1 \
--table stats_order --export-dir /hive/log_phone.db/stats_order_tmp1/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,success_orders,refund_orders,created' \
;

####将三种金额的计算写成一条语句
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
where p.month = 9
and p.day = 24
and p.oid is not null
and p.oid <> 'null'
and p.en = 'e_crt'
group by from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),p.pl,p.cut,p.pt,p.en,s.en,r.en
) as tmp
insert overwrite table stats_order_tmp2
select phone_date(dt),phone_platform(pl),phone_currency(cut),phone_pay(pt),sum(orders_amount),sum(success_orders_amount),sum(refund_orders_amount),dt
group by dt,pl,cut,pt
;

sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root -m 1 \
--table stats_order --export-dir /hive/log_phone.db/stats_order_tmp2/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id,order_amount,revenue_amount,refund_amount,created' \
;
