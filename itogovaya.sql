SET search_path TO bookings;


--1 � ����� ������� ������ ������ ���������

select a.city , count(*) as cnt
from airports a 
group by a.city
having count (*) > 1 --������  ������ � ���-���� ���������� ����� 1
 

--2 � ����� ���������� ���� �����, ����������� ��������� � ������������ ���������� ��������?

  select distinct (a.airport_name), a.city,a.airport_code, t.rannge 
from (
		 select aircraft_code, max(ai.range) as rannge -- (1)������� 1 ���� ��������
		 FROM aircrafts ai
		 group by ai.aircraft_code
		 ORDER BY max(ai.range) desc limit 1 --(1)
		 ) t
 join flights f on f.aircraft_code =t.aircraft_code -- ���������� � ��������  �������� 
 join airports a on a.airport_code = f.departure_airport  or a.airport_code=f.arrival_airport --��������� � �������� � �����������
 
 
 
 --3 ������� 10 ������ � ������������ �������� �������� ������
--explain analyze

 select flight_id , flight_no,  departure_airport, arrival_airport, (actual_departure - scheduled_departure) as delay--, scheduled_departure,actual_departure
 from flights f 
 where actual_departure IS NOT NULL -- ������������� �� ���� ������ 
 ORDER BY delay desc limit 10
 
 
--4 ���� �� �����, �� ������� �� ���� �������� ���������� ������?
-- explain analyze 

 select  count(distinct book_ref ) --  �������� ��� ������ � ������� �� ���� ������ ����������
from boarding_passes bp 
right outer join  tickets t2  using (ticket_no)
where bp.boarding_no is null
			
			
			
			
-- 5 ������� ���������� ��������� ���� ��� ������� �����, �� % ��������� � ������ ���������� ���� � ��������.
--�������� ������� � ������������� ������ - ��������� ���������� ���������� ���������� ���������� �� ������� ��������� �� ������ ����. 
--�.�. � ���� ������� ������ ���������� ������������� ����� - ������� ������� ��� �������� �� ������� ��������� �� ���� ��� ����� ������ ������ � ������� ���.

with c1 as (
		select distinct (aircraft_code) , count(seat_no) over (partition by s.aircraft_code) as all_seats -- ����� ���-�� ���� � ��������
		from seats s
		group by aircraft_code, seat_no			
), 	c2 as ( 
		select  distinct (f.flight_id), f.flight_no, f.actual_departure, f.departure_airport , f.aircraft_code, c1.all_seats,
		count(seat_no) over (partition by f.flight_id order by f.aircraft_code )as book_seat --(1) ���-�� ������� ����
		from  flights f 
		join boarding_passes bp using (flight_id)--���������� ��������������� ����� � ��������
		join c1 on c1.aircraft_code = f.aircraft_code --������������ ����� ���-�� ���� � ��������
		where f.status ilike  'arrived' or status ilike 'Departed' --(1) ����� �� ������� � ����������� ������������
		group by f.flight_id, f.flight_no, f.actual_departure, f.departure_airport , f.aircraft_code,bp.seat_no, c1.all_seats -- ������������� �� �������� ��� ���������� ��������
) 
 select c2.flight_id, c2.all_seats,c2.book_seat,
 	(c2.all_seats-c2.book_seat) as free_seats, -- ��������� �����
 	(c2.all_seats-c2.book_seat)*100/c2.all_seats   as procent_free, --% ��������� ��������� ���� � ������ ���������� ���� � ��������
 	sum(book_seat) over (partition by c2.departure_airport, date_trunc('days', c2.actual_departure) order by c2.actual_departure) as YTD_day-- ����� ����������� ������ �� ���������� ���������� � ������� ��� �� ������ ����������,
from c2
	


-- 6 ������� ���������� ����������� ��������� �� ����� ��������� �� ������ ����������.
 
select distinct (f.aircraft_code),
	count (*) over() as count_reis,--����� ���-�� ������
	count(flight_no) over(partition by  f.aircraft_code) as  count_air, --���-�� ��������� ������� ��������
	cast( ROUND ( count(flight_no) over(partition by  f.aircraft_code)*100.0/count (*) over(),0)as numeric) as procent --���������� ��������� �� ����� ��������� �� ������ ����������
from flights f



--7 ���� �� ������, � ������� �����  ��������� ������ - ������� �������, ��� ������-������� � ������ ��������
--explain analyse		
with tc1 as (
			select  distinct (flight_id) , fare_conditions , amount --������������� ������ ������ ������
			from ticket_flights tf 
			where fare_conditions ilike 'economy'
	),tc2 as (
			select distinct(tf.flight_id) , tf.fare_conditions , tf.amount as business_amount, tc1.fare_conditions, tc1.amount as econom_amount
			from ticket_flights tf 
			inner join tc1 on tc1.flight_id= tf.flight_id --����������� ������ �� ��� ���������, � ������� ���� � ������ � ������� �����
			where tf.fare_conditions ilike 'business' -- ������ ������ ������� ������
	)
select fv.flight_id, fv.flight_no, fv.departure_city, fv.arrival_city,tc2.business_amount, tc2.econom_amount
from flights_v fv
join tc2 on tc2.flight_id=fv.flight_id --���������� �������� �� ���������� ������� ��� ������ ������ �������
where tc2.business_amount < tc2.econom_amount --����������� �� �������



--8.����� ������ �������� ��� ������ ������?

create view aircity as
select *    -- ��������� ������������ ��� cross join
from (
	select  a.city as depCity, a2.city as arrivCity
	from airports a,  airports a2
	where a.city <> a2.city 
	) t
	except 
	select  a.city, a2.city  
from airports a
inner join flights f on a.airport_code=f.departure_airport --��������� ������ �����������, ������� ���� � ����������
inner join airports a2 on a2.airport_code=f.arrival_airport -- ��������� ������ �������, ������� ���� � ����������

select *, count(*) over() 
from aircity t



--9.��������� ���������� ����� �����������, ���������� ������� �������, �������� � ���������� ������������ ���������� ���������  � ���������, 
--.������������� ��� �����


select a.city as departure_airport,a2.city as arrival_city,a3.model,a3."range" as air_range,
	round( acos(sind(a.latitude)*sind(a2.latitude) + cosd(a.latitude)*cosd(a2.latitude)*cosd(a.longitude-a2.longitude))*6371::numeric) as city_distance, 
		case when round( acos(sind(a.latitude)*sind(a2.latitude) + cosd(a.latitude)*cosd(a2.latitude)*cosd(a.longitude-a2.longitude))*6371::numeric) <= a3."range" 
			 then '�������' 
			 else '�� �������' 
		end  
FROM airports a JOIN airports a2
 ON a.city <> a2.city  --������� ����������� ������������ ���� ��������� ��������� ����� �������
cross join aircrafts a3 --��� ��������� ���������� ��������� � ������� �������


