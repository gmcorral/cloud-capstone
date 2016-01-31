# Group 1 ex 1
select o.origin as airport, o.flight_nr + d.flight_nr as total from

(select origin, count(origin) as flight_nr from airline_ontime
where cancelled = 0
group by origin) as o,

(select dest, count(dest) as flight_nr from airline_ontime
where cancelled = 0
group by dest) as d

where o.origin = d.dest
order by total desc limit 10;

# Group 1 ex 2
select carrier, sum(arrdelay)/count(arrdelay) as mean_delay from airline_ontime
where cancelled = 0
group by carrier order by mean_delay asc limit 10;

# Group 1 ex 3
select weekday, sum(arrdelay)/count(arrdelay) as mean_delay from airline_ontime
where cancelled = 0
group by weekday order by mean_delay asc;

# Group 2 ex 1
insert overwrite table group2_ex1
select origin as airport, carrier, sum(depdelay)/count(depdelay) as mean_delay from airline_ontime
where cancelled = 0
group by origin, carrier;

# Group 2 ex 2
insert overwrite table group2_ex2
select origin as airport, dest as destination, sum(depdelay)/count(depdelay) as mean_delay from airline_ontime
where cancelled = 0
group by origin, dest;

# Group 2 ex 4
insert overwrite table group2_ex4
select origin, dest as destination, sum(arrdelay)/count(arrdelay) as mean_delay from airline_ontime
where cancelled = 0
group by origin, dest;

# Group 3 ex 1
select o.origin as airport, o.flight_nr + d.flight_nr as popularity from

(select origin, count(origin) as flight_nr from airline_ontime
where cancelled = 0
group by origin) as o,

(select dest, count(dest) as flight_nr from airline_ontime
where cancelled = 0
group by dest) as d

where o.origin = d.dest
order by popularity desc;

# group 3 ex 2

drop table if exists flights_xy;
create table flights_xy as
select 
    origin as x,
    dest as y,
    flight_num,
    date,
    deptime,
    arrdelay + depdelay as delay,
    carrier
from airline_ontime
where cancelled = 0 and deptime < "1200" and date like '2008-%';

drop table if exists flights_yz;
create table flights_yz as
select
    origin as y,
    dest as z,
    flight_num,
    date,
    deptime,
    arrdelay + depdelay as delay,
    carrier
from airline_ontime
where cancelled = 0 and deptime > "1200" and date like '2008-%';

#set mapreduce.job.reduces=4;

drop table if exists flights_xyz;
create table flights_xyz as
select
    concat(xy.x, "-", xy.y, "-", yz.z) as route,
    xy.date as depdate,
    concat(xy.carrier, xy.flight_num) as flight_xy,
    concat(yz.carrier, yz.flight_num) as flight_yz,
    xy.delay + yz.delay as delay
from
    flights_xy as xy,
    flights_yz as yz
where xy.y = yz.y and yz.date = date_add(xy.date, 2);

insert overwrite table group3_ex2
select xyz.route, xyz.depdate, xyz.flight_xy, xyz.flight_yz from flights_xyz xyz
inner join (
    select route, depdate, min(delay) as min_delay
    from flights_xyz
    group by route, depdate
) xyz_min
on  xyz.route = xyz_min.route and
    xyz.depdate = xyz_min.depdate
where xyz.delay = xyz_min.min_delay;