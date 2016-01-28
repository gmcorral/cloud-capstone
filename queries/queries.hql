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
group by airport, carrier order by mean_delay asc;

# Group 2 ex 2
insert overwrite table group2_ex2
select origin as airport, dest as destination, sum(depdelay)/count(depdelay) as mean_delay from airline_ontime
where cancelled = 0
group by origin, dest order by mean_delay asc;

# Group 2 ex 4
insert overwrite table group2_ex4
select origin, dest as destination, sum(arrdelay)/count(arrdelay) as mean_delay from airline_ontime
where cancelled = 0
group by origin, dest order by origin, dest asc;

