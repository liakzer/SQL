SET search_path TO bookings;


--1 В каких городах больше одного аэропорта.

select a.city , count(*) as cnt
from airports a 
group by a.city
having count (*) > 1 --вывела  строки с кол-вотм аэропортов более 1
 

--2 В каких аэропортах есть рейсы, выполняемые самолетом с максимальной дальностью перелета?

  select distinct (a.airport_name), a.city,a.airport_code, t.rannge 
from (
		 select aircraft_code, max(ai.range) as rannge -- (1)выбрала 1 макс значение
		 FROM aircrafts ai
		 group by ai.aircraft_code
		 ORDER BY max(ai.range) desc limit 1 --(1)
		 ) t
 join flights f on f.aircraft_code =t.aircraft_code -- объединила с таблицей  перелеты 
 join airports a on a.airport_code = f.departure_airport  or a.airport_code=f.arrival_airport --обединила с перелеты с аэропортами
 
 
 
 --3 Вывести 10 рейсов с максимальным временем задержки вылета
--explain analyze

 select flight_id , flight_no,  departure_airport, arrival_airport, (actual_departure - scheduled_departure) as delay--, scheduled_departure,actual_departure
 from flights f 
 where actual_departure IS NOT NULL -- отфильтровала по дате вылета 
 ORDER BY delay desc limit 10
 
 
--4 Были ли брони, по которым не были получены посадочные талоны?
-- explain analyze 

 select  count(distinct book_ref ) --  отобрала все билеты к которым не были выданы посадочные
from boarding_passes bp 
right outer join  tickets t2  using (ticket_no)
where bp.boarding_no is null
			
			
			
			
-- 5 Найдите количество свободных мест для каждого рейса, их % отношение к общему количеству мест в самолете.
--Добавьте столбец с накопительным итогом - суммарное накопление количества вывезенных пассажиров из каждого аэропорта на каждый день. 
--Т.е. в этом столбце должна отражаться накопительная сумма - сколько человек уже вылетело из данного аэропорта на этом или более ранних рейсах в течении дня.

with c1 as (
		select distinct (aircraft_code) , count(seat_no) over (partition by s.aircraft_code) as all_seats -- всего кол-во мест в самолете
		from seats s
		group by aircraft_code, seat_no			
), 	c2 as ( 
		select  distinct (f.flight_id), f.flight_no, f.actual_departure, f.departure_airport , f.aircraft_code, c1.all_seats,
		count(seat_no) over (partition by f.flight_id order by f.aircraft_code )as book_seat --(1) кол-во занятых мест
		from  flights f 
		join boarding_passes bp using (flight_id)--объединила забронированные места и перелеты
		join c1 on c1.aircraft_code = f.aircraft_code --присоединили общее кол-во мест в самолете
		where f.status ilike  'arrived' or status ilike 'Departed' --(1) отбор по вылетам с законченной регистрацией
		group by f.flight_id, f.flight_no, f.actual_departure, f.departure_airport , f.aircraft_code,bp.seat_no, c1.all_seats -- сгруппировали по столбцам для дальнейших расчетов
) 
 select c2.flight_id, c2.all_seats,c2.book_seat,
 	(c2.all_seats-c2.book_seat) as free_seats, -- свободные места
 	(c2.all_seats-c2.book_seat)*100/c2.all_seats   as procent_free, --% отношение свободных мест к общему количеству мест в самолете
 	sum(book_seat) over (partition by c2.departure_airport, date_trunc('days', c2.actual_departure) order by c2.actual_departure) as YTD_day-- сумма нарастающим итогоу по вылетевшим пассажирам в течение дня из одного аэропрорта,
from c2
	


-- 6 Найдите процентное соотношение перелетов по типам самолетов от общего количества.
 
select distinct (f.aircraft_code),
	count (*) over() as count_reis,--общее кол-во рейсов
	count(flight_no) over(partition by  f.aircraft_code) as  count_air, --кол-во перелетов каждого самолета
	cast( ROUND ( count(flight_no) over(partition by  f.aircraft_code)*100.0/count (*) over(),0)as numeric) as procent --процентное перелетов по типам самолетов от общего количества
from flights f



--7 Были ли города, в которые можно  добраться бизнес - классом дешевле, чем эконом-классом в рамках перелета
--explain analyse		
with tc1 as (
			select  distinct (flight_id) , fare_conditions , amount --отфильтровала билеты эконом класса
			from ticket_flights tf 
			where fare_conditions ilike 'economy'
	),tc2 as (
			select distinct(tf.flight_id) , tf.fare_conditions , tf.amount as business_amount, tc1.fare_conditions, tc1.amount as econom_amount
			from ticket_flights tf 
			inner join tc1 on tc1.flight_id= tf.flight_id --объединение только по тем перелетам, в которых есть и эконом и бизнесс класс
			where tf.fare_conditions ilike 'business' -- билеты только бизнесс класса
	)
select fv.flight_id, fv.flight_no, fv.departure_city, fv.arrival_city,tc2.business_amount, tc2.econom_amount
from flights_v fv
join tc2 on tc2.flight_id=fv.flight_id --объединила перелеты со стоимостью билетов для вывода списка городов
where tc2.business_amount < tc2.econom_amount --фитльтрация по условию



--8.Между какими городами нет прямых рейсов?

create view aircity as
select *    -- декартово произведение без cross join
from (
	select  a.city as depCity, a2.city as arrivCity
	from airports a,  airports a2
	where a.city <> a2.city 
	) t
	except 
	select  a.city, a2.city  
from airports a
inner join flights f on a.airport_code=f.departure_airport --исключили города отправления, которые есть в расписании
inner join airports a2 on a2.airport_code=f.arrival_airport -- исключили города прилета, которые есть в расписании

select *, count(*) over() 
from aircity t



--9.Вычислите расстояние между аэропортами, связанными прямыми рейсами, сравните с допустимой максимальной дальностью перелетов  в самолетах, 
--.обслуживающих эти рейсы


select a.city as departure_airport,a2.city as arrival_city,a3.model,a3."range" as air_range,
	round( acos(sind(a.latitude)*sind(a2.latitude) + cosd(a.latitude)*cosd(a2.latitude)*cosd(a.longitude-a2.longitude))*6371::numeric) as city_distance, 
		case when round( acos(sind(a.latitude)*sind(a2.latitude) + cosd(a.latitude)*cosd(a2.latitude)*cosd(a.longitude-a2.longitude))*6371::numeric) <= a3."range" 
			 then 'Долетит' 
			 else 'Не долетит' 
		end  
FROM airports a JOIN airports a2
 ON a.city <> a2.city  --создала декартового произведение всех возможных маршрутов между городам
cross join aircrafts a3 --все возможные комбинации самолетов и городов вылетов


