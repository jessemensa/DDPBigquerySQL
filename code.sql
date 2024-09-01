-- DESCRIPTIVE, DIAGNOSTIC AND PREDICTIVE ANALYTICS IN BIGQUERY 

-- TODO: COLUMNS AND MEANINGS 
-- COLUMNS AND MEANINGS 

-- num_passengers = number of passengers travelling
-- sales_channel = sales channel booking was made on
-- trip_type = trip Type (Round Trip, One Way, Circle Trip)
-- purchase_lead = number of days between travel date and booking date
-- length_of_stay = number of days spent at destination
-- flight_hour = hour of flight departure
-- flight_day = day of week of flight departure
-- route = origin -> destination flight route
-- booking_origin = country from where booking was made
-- wants_extra_baggage = if the customer wanted extra baggage in the booking
-- wants_preferred_seat = if the customer wanted a preferred seat in the booking
-- wants_in_flight_meals = if the customer wanted in-flight meals in the booking
-- flight_duration = total duration of flight (in hours)
-- booking_complete = flag indicating if the customer completed the booking



--A DATA QUALITY CHECK 
--1a. missing values 
SELECT 
  SUM(CASE WHEN num_passengers IS NULL THEN 1 ELSE 0 END) AS missing_num_passengers,
  SUM(CASE WHEN sales_channel IS NULL THEN 1 ELSE 0 END) AS missing_sales_channel,
  SUM(CASE WHEN trip_type IS NULL THEN 1 ELSE 0 END) AS missing_trip_type,
  SUM(CASE WHEN purchase_lead IS NULL THEN 1 ELSE 0 END) AS missing_purchase_lead,
  SUM(CASE WHEN length_of_stay IS NULL THEN 1 ELSE 0 END) AS missing_length_of_stay,
  SUM(CASE WHEN flight_hour IS NULL THEN 1 ELSE 0 END) AS missing_flight_hour,
  SUM(CASE WHEN flight_day IS NULL THEN 1 ELSE 0 END) AS missing_flight_day,
  SUM(CASE WHEN route IS NULL THEN 1 ELSE 0 END) AS missing_route,
  SUM(CASE WHEN booking_origin IS NULL THEN 1 ELSE 0 END) AS missing_booking_origin,
  SUM(CASE WHEN wants_extra_baggage IS NULL THEN 1 ELSE 0 END) AS missing_wants_extra_baggage,
  SUM(CASE WHEN wants_preferred_seat IS NULL THEN 1 ELSE 0 END) AS missing_wants_preferred_seat,
  SUM(CASE WHEN wants_in_flight_meals IS NULL THEN 1 ELSE 0 END) AS missing_wants_in_flight_meals,
  SUM(CASE WHEN flight_duration IS NULL THEN 1 ELSE 0 END) AS missing_flight_duration,
  SUM(CASE WHEN booking_complete IS NULL THEN 1 ELSE 0 END) AS missing_booking_complete
FROM 
  `youtubescripts-425810.youtubebigquery.customerbooking`;

--2a. duplicate records 
SELECT 
  num_passengers,
  sales_channel,
  trip_type,
  purchase_lead,
  length_of_stay,
  flight_hour,
  flight_day,
  route,
  booking_origin,
  wants_extra_baggage,
  wants_preferred_seat,
  wants_in_flight_meals,
  flight_duration,
  booking_complete,
  COUNT(*) AS record_count
FROM 
  `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY 
  num_passengers,
  sales_channel,
  trip_type,
  purchase_lead,
  length_of_stay,
  flight_hour,
  flight_day,
  route,
  booking_origin,
  wants_extra_baggage,
  wants_preferred_seat,
  wants_in_flight_meals,
  flight_duration,
  booking_complete
HAVING 
  COUNT(*) > 1
ORDER BY 
  record_count DESC;



  


--B DESCRIPTIVE ANALYTICS 
-- which continents take more round trips, one way and circle trips 

--1b. daily booking trends 
select flight_day,
       count(*) as num_bookings from `youtubescripts-425810.youtubebigquery.customerbooking`
       group by flight_day
       order by num_bookings DESC;


--2b. sales channel performance 
select sales_channel, 
       count(*) as num_bookings, 
       sum(booking_complete) as completed_bookings    
       from  `youtubescripts-425810.youtubebigquery.customerbooking`        
       group by sales_channel; 
       

--3b. num of bookings and completed bookings rate 
SELECT   
   sales_channel, 
   count(*) as total_bookings, 
   sum(booking_complete) as completed_bookings, 
   round(count(*) / (select count(*) from `youtubescripts-425810.youtubebigquery.customerbooking`) * 100, 1) as bookings_rate, 
   round(sum(booking_complete) / count(*) * 100, 1) as completed_booking_rate 
   from `youtubescripts-425810.youtubebigquery.customerbooking`
   group by sales_channel 
   order by total_bookings DESC; 

-- this has a question mark against it 
--4b. customer performance analysis 
SELECT      
    count(*) as total_bookings, 
    sum(wants_extra_baggage) as extra_baggage, 
    round(sum(wants_extra_baggage) / count(*) * 100, 2) as extra_baggage_rate, 
    sum(wants_preferred_seat) as preferred_seat, 
    round(sum(wants_preferred_seat) / count(*) * 100, 2) as wants_preferred_seat
    from `youtubescripts-425810.youtubebigquery.customerbooking`; 


--5b. trip type analysis 
SELECT     
     trip_type, 
     count(*) as num_bookings    
     from `youtubescripts-425810.youtubebigquery.customerbooking`
     group by trip_type         
     order by num_bookings DESC; 


--6b. lead time analysis 
SELECT     
     purchase_lead, 
     count(*) as num_bookings, 
     sum(booking_complete) as completed_bookings
     FROM 
     `youtubescripts-425810.youtubebigquery.customerbooking`
     group by     
     purchase_lead   
     order by   
     purchase_lead; 


--7b. length of stay analysis 
SELECT    
   length_of_stay, 
   count(*) as num_bookings                 
   FROM `youtubescripts-425810.youtubebigquery.customerbooking`       
   GROUP BY length_of_stay       
   ORDER BY num_bookings DESC; 

--8b. flight timing analysis 
SELECT    
   flight_hour, 
   count(*) as num_bookings                 
   FROM `youtubescripts-425810.youtubebigquery.customerbooking`       
   GROUP BY flight_hour     
   ORDER BY num_bookings DESC;    
     
--9b. route analysis 
SELECT    
   route, 
   count(*) as num_bookings                 
   FROM `youtubescripts-425810.youtubebigquery.customerbooking`       
   GROUP BY route    
   ORDER BY num_bookings DESC;    

--10b. booking origin 
SELECT    
   booking_origin, 
   count(*) as num_bookings                 
   FROM `youtubescripts-425810.youtubebigquery.customerbooking`       
   GROUP BY booking_origin    
   ORDER BY num_bookings DESC;  


--11b. flight duration analysis 
SELECT         
     route, 
     AVG(flight_duration) as avg_duration    
     FROM  `youtubescripts-425810.youtubebigquery.customerbooking`
     GROUP BY route
     ORDER BY avg_duration DESC; 



--12b. sales channel performance performance with booking completion rate 
SELECT    
    sales_channel, 
    count(*) as total_bookings, 
    sum(booking_complete) as completed_bookings, 
    round(sum(booking_complete) / COUNT(*) * 100, 2)  as completion_rate,
    AVG(length_of_stay) as avg_length_of_stay

    FROM `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY sales_channel  
ORDER BY total_bookings DESC; 



--13b. lead time and booking completion analysis 
-- purchase lead with more than 100 bookings 
SELECT   
     purchase_lead,      
     COUNT(*) AS total_bookings,        
     SUM(booking_complete) AS completed_bookings, 
     ROUND(SUM(booking_complete) / count(*) * 100, 2) as completion_rate        
     FROM `youtubescripts-425810.youtubebigquery.customerbooking` 
     GROUP BY    
     purchase_lead 
     having count(*) > 100   
     order by purchase_lead; 

--14b. popular routes with flight duration and booking completion using CTE 
WITH route_analysis as (
select 
     route, 
     count(*) as total_bookings, 
     avg(flight_duration) as avg_flight_duration, 
     sum(case when booking_complete = 1 then 1 else 0 end) as completed_bookings, 
     round(sum(case when booking_complete = 1 then 1 else 0 end) / count(*) * 100, 2) as completion_rate
     FROM `youtubescripts-425810.youtubebigquery.customerbooking`              
     GROUP BY route 
) 
select 
  route, 
  total_bookings, 
  avg_flight_duration, 
  completed_bookings, 
  completion_rate    
  from route_analysis 
  order by 
  total_bookings DESC;



--15b. identify the routes with the highest and lowest booking completion rates 
with data_origin as (
SELECT 
booking_origin, 
count(*) as total_bookings,  
sum(booking_complete) as completed_bookings, 
round(sum(case when booking_complete = 1 then 1 else 0 end) / count(*) * 100, 2) as completion_rate, 
sum(case when wants_extra_baggage = 1 then 1 else 0 end) as total_extra_baggage, 
sum(case when wants_preferred_seat = 1 then 1 else 0 end) as total_preferred_seat, 
sum(case when wants_in_flight_meals = 1 then 1 else 0 end) as total_in_flight_meals   
FROM `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY booking_origin
) 
SELECT    
   booking_origin, 
   total_bookings, 
   completed_bookings, 
   completion_rate, 
   total_extra_baggage, 
   total_preferred_seat, 
   total_in_flight_meals, 
   round(total_extra_baggage / total_bookings * 100, 2) as extra_baggage_rate, 
   round(total_preferred_seat / total_bookings * 100, 2) as preferred_seat_rate, 

   from data_origin 
   order by total_bookings DESC; 









-- 16B.: Investigate the impact of purchase lead time on booking completion
SELECT
  CASE
    WHEN purchase_lead <= 30 THEN '0-30 days'
    WHEN purchase_lead <= 60 THEN '31-60 days'
    WHEN purchase_lead <= 90 THEN '61-90 days'
    ELSE '90+ days'
  END AS lead_time_category,
  COUNT(*) AS total_bookings,
  SUM(booking_complete) AS completed_bookings,
  ROUND(SUM(booking_complete) / COUNT(*) * 100, 2) AS completion_rate
FROM
  `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY
  lead_time_category;


-- 17B.: Analyze additional services and booking completion
SELECT
  CASE
    WHEN wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals = 0 THEN 'No additional services'
    WHEN wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals = 1 THEN '1 additional service'
    WHEN wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals = 2 THEN '2 additional services'
    ELSE '3 additional services'
  END AS additional_services,
  COUNT(*) AS total_bookings,
  SUM(booking_complete) AS completed_bookings,
  ROUND(SUM(booking_complete) / COUNT(*) * 100, 2) AS completion_rate
FROM
  `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY
  additional_services;


-- 18B: Investigate the impact of flight duration on booking completion
SELECT
  CASE
    WHEN flight_duration < 5 THEN 'Less than 5 hours'
    WHEN flight_duration < 10 THEN '5-10 hours'
    ELSE '10+ hours'
  END AS flight_duration_category,
  COUNT(*) AS total_bookings,
  SUM(booking_complete) AS completed_bookings,
  ROUND(SUM(booking_complete) / COUNT(*) * 100, 2) AS completion_rate
FROM
  `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY
  flight_duration_category;



-- 19B: Analyze the relationship between booking origin and additional services
SELECT
  booking_origin,
  ROUND(AVG(wants_extra_baggage) * 100, 2) AS extra_baggage_percentage,
  ROUND(AVG(wants_preferred_seat) * 100, 2) AS preferred_seat_percentage,
  ROUND(AVG(wants_in_flight_meals) * 100, 2) AS in_flight_meals_percentage
FROM
  `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY
  booking_origin;


-- 20B: Investigate the impact of flight day on booking completion rate
SELECT
  flight_day,
  COUNT(*) AS total_bookings,
  SUM(booking_complete) AS completed_bookings,
  ROUND(SUM(booking_complete) / COUNT(*) * 100, 2) AS completion_rate
FROM
  `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY
  flight_day
ORDER BY
  CASE flight_day
    WHEN 'Mon' THEN 1
    WHEN 'Tue' THEN 2
    WHEN 'Wed' THEN 3
    WHEN 'Thu' THEN 4
    WHEN 'Fri' THEN 5
    WHEN 'Sat' THEN 6
    WHEN 'Sun' THEN 7
  END;




-- DIANOSTIC ANALYTICS 
-- do longer flights have more requests for additional services ?


WITH service_counts AS (
  SELECT
    BOOKING_ORIGIN,
    CASE
      WHEN wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals = 0 THEN 'No additional services'
      WHEN wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals = 1 THEN '1 additional service'
      WHEN wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals = 2 THEN '2 additional services'
      ELSE '3 additional services'
    END AS additional_services,
    COUNT(*) AS total_bookings,
    SUM(booking_complete) AS completed_bookings,
    SUM(flight_duration) AS total_flight_duration
  FROM
    `youtubescripts-425810.youtubebigquery.customerbooking`
  GROUP BY
    BOOKING_ORIGIN, additional_services
)

SELECT
  BOOKING_ORIGIN,
  SUM(CASE WHEN additional_services = 'No additional services' THEN total_bookings ELSE 0 END) AS no_additional_services,
  SUM(CASE WHEN additional_services = '1 additional service' THEN total_bookings ELSE 0 END) AS one_additional_service,
  SUM(CASE WHEN additional_services = '2 additional services' THEN total_bookings ELSE 0 END) AS two_additional_services,
  SUM(CASE WHEN additional_services = '3 additional services' THEN total_bookings ELSE 0 END) AS three_additional_services,
  SUM(total_bookings) AS total_bookings,
  SUM(completed_bookings) AS completed_bookings,
  ROUND(SUM(completed_bookings) / SUM(total_bookings) * 100, 2) AS completion_rate,
  SUM(total_flight_duration) AS total_flight_duration,
  ROUND(SUM(total_flight_duration) / SUM(total_bookings), 2) AS avg_flight_duration
FROM
  service_counts
GROUP BY
  BOOKING_ORIGIN
ORDER BY
  total_flight_duration DESC;


-- Question: do passengers who book further in advance tend to request more extra services?

WITH lead_time_categories AS (
  SELECT
    *,
    CASE
      WHEN purchase_lead <= 30 THEN 'Short (0-30 days)'
      WHEN purchase_lead <= 90 THEN 'Medium (31-90 days)'
      ELSE 'Long (91+ days)'
    END AS lead_time_category,
    wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals AS total_extra_services
  FROM
    `youtubescripts-425810.youtubebigquery.customerbooking`
)

SELECT
  lead_time_category,
  COUNT(*) AS total_bookings,
  SUM(total_extra_services) AS total_services_requested,
  ROUND(AVG(total_extra_services), 2) AS avg_services_per_booking,
  SUM(CASE WHEN total_extra_services > 0 THEN 1 ELSE 0 END) AS bookings_with_extra_services,
  ROUND(SUM(CASE WHEN total_extra_services > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_bookings_with_extra_services
FROM
  lead_time_categories
GROUP BY
  lead_time_category
ORDER BY
  CASE
    WHEN lead_time_category = 'Short (0-30 days)' THEN 1
    WHEN lead_time_category = 'Medium (31-90 days)' THEN 2
    ELSE 3
  END;


-- Question: do larger groups tend to book further in advance?

WITH group_size_categories AS (
  SELECT
    CASE
      WHEN num_passengers = 1 THEN 'Single'
      WHEN num_passengers = 2 THEN 'Couple'
      WHEN num_passengers BETWEEN 3 AND 5 THEN 'Small Group (3-5)'
      ELSE 'Large Group (6+)'
    END AS group_size,
    purchase_lead
  FROM
    `youtubescripts-425810.youtubebigquery.customerbooking`
)

SELECT
  group_size,
  COUNT(*) AS total_bookings,
  ROUND(AVG(purchase_lead), 2) AS avg_purchase_lead,
  MIN(purchase_lead) AS min_purchase_lead,
  MAX(purchase_lead) AS max_purchase_lead,
  ROUND(STDDEV(purchase_lead), 2) AS stddev_purchase_lead,
  ROUND(
    AVG(CASE WHEN purchase_lead > 30 THEN 1 ELSE 0 END) * 100,
    2
  ) AS pct_bookings_over_30_days
FROM
  group_size_categories
GROUP BY
  group_size
ORDER BY
  CASE
    WHEN group_size = 'Single' THEN 1
    WHEN group_size = 'Couple' THEN 2
    WHEN group_size = 'Small Group (3-5)' THEN 3
    ELSE 4
  END;



-- * Question: do flights on certain days of the week correlate with longer stays?


WITH day_order AS (
  SELECT 'Mon' AS day, 1 AS day_order UNION ALL
  SELECT 'Tue', 2 UNION ALL
  SELECT 'Wed', 3 UNION ALL
  SELECT 'Thu', 4 UNION ALL
  SELECT 'Fri', 5 UNION ALL
  SELECT 'Sat', 6 UNION ALL
  SELECT 'Sun', 7
)

SELECT
  cb.flight_day,
  COUNT(*) AS total_bookings,
  ROUND(AVG(cb.length_of_stay), 2) AS avg_length_of_stay,
  MIN(cb.length_of_stay) AS min_length_of_stay,
  MAX(cb.length_of_stay) AS max_length_of_stay,
  ROUND(STDDEV(cb.length_of_stay), 2) AS stddev_length_of_stay,
  ROUND(AVG(CASE WHEN cb.length_of_stay > 7 THEN 1 ELSE 0 END) * 100, 2) AS pct_stays_over_week,
  ROUND(AVG(cb.wants_extra_baggage + cb.wants_preferred_seat + cb.wants_in_flight_meals), 2) AS avg_extra_services
FROM
  `youtubescripts-425810.youtubebigquery.customerbooking` cb
JOIN
  day_order ON cb.flight_day = day_order.day
GROUP BY
  cb.flight_day, day_order.day_order
ORDER BY
  day_order.day_order;




-- Question: do passengers on early morning or late night flights have different service request patterns?


WITH flight_hour_categories AS (
  SELECT
    *,
    CASE
      WHEN flight_hour BETWEEN 0 AND 5 THEN 'Late Night (00:00-05:59)'
      WHEN flight_hour BETWEEN 6 AND 11 THEN 'Morning (06:00-11:59)'
      WHEN flight_hour BETWEEN 12 AND 17 THEN 'Afternoon (12:00-17:59)'
      ELSE 'Evening (18:00-23:59)'
    END AS flight_time_category
  FROM
    `youtubescripts-425810.youtubebigquery.customerbooking`
)

SELECT
  flight_time_category,
  COUNT(*) AS total_bookings,
  ROUND(AVG(wants_extra_baggage) * 100, 2) AS pct_extra_baggage,
  ROUND(AVG(wants_preferred_seat) * 100, 2) AS pct_preferred_seat,
  ROUND(AVG(wants_in_flight_meals) * 100, 2) AS pct_in_flight_meals,
  ROUND(AVG(wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals), 2) AS avg_services_per_booking,
  ROUND(AVG(CASE WHEN wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals > 0 THEN 1 ELSE 0 END) * 100, 2) AS pct_bookings_with_any_service
FROM
  flight_hour_categories
GROUP BY
  flight_time_category
ORDER BY
  CASE
    WHEN flight_time_category = 'Late Night (00:00-05:59)' THEN 1
    WHEN flight_time_category = 'Morning (06:00-11:59)' THEN 2
    WHEN flight_time_category = 'Afternoon (12:00-17:59)' THEN 3
    ELSE 4
  END;

-- Question: do mobile bookings tend to have different lead times compared to internet bookings?

WITH lead_time_categories AS (
  SELECT
    *,
    CASE
      WHEN purchase_lead <= 7 THEN 'Last Minute (0-7 days)'
      WHEN purchase_lead <= 30 THEN 'Short (8-30 days)'
      WHEN purchase_lead <= 90 THEN 'Medium (31-90 days)'
      ELSE 'Long (91+ days)'
    END AS lead_time_category
  FROM
    `youtubescripts-425810.youtubebigquery.customerbooking`
)

SELECT
  sales_channel,
  COUNT(*) AS total_bookings,
  ROUND(AVG(purchase_lead), 2) AS avg_purchase_lead,
  MIN(purchase_lead) AS min_purchase_lead,
  MAX(purchase_lead) AS max_purchase_lead,
  ROUND(STDDEV(purchase_lead), 2) AS stddev_purchase_lead,
  ROUND(AVG(CASE WHEN lead_time_category = 'Last Minute (0-7 days)' THEN 1 ELSE 0 END) * 100, 2) AS pct_last_minute,
  ROUND(AVG(CASE WHEN lead_time_category = 'Short (8-30 days)' THEN 1 ELSE 0 END) * 100, 2) AS pct_short_lead,
  ROUND(AVG(CASE WHEN lead_time_category = 'Medium (31-90 days)' THEN 1 ELSE 0 END) * 100, 2) AS pct_medium_lead,
  ROUND(AVG(CASE WHEN lead_time_category = 'Long (91+ days)' THEN 1 ELSE 0 END) * 100, 2) AS pct_long_lead,
  ROUND(AVG(wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals), 2) AS avg_services_requested
FROM
  lead_time_categories
GROUP BY
  sales_channel
ORDER BY
  sales_channel;



-- * Question: do passengers with longer stays tend to request extra baggage more often?



WITH stay_length_categories AS (
  SELECT
    *,
    CASE
      WHEN length_of_stay <= 3 THEN 'Short Stay (1-3 days)'
      WHEN length_of_stay <= 7 THEN 'Medium Stay (4-7 days)'
      WHEN length_of_stay <= 14 THEN 'Long Stay (8-14 days)'
      ELSE 'Extended Stay (15+ days)'
    END AS stay_category
  FROM
    `youtubescripts-425810.youtubebigquery.customerbooking`
)

SELECT
  stay_category,
  COUNT(*) AS total_bookings,
  ROUND(AVG(length_of_stay), 2) AS avg_length_of_stay,
  ROUND(AVG(wants_extra_baggage) * 100, 2) AS pct_extra_baggage,
  ROUND(AVG(CASE WHEN wants_extra_baggage = 1 THEN length_of_stay ELSE NULL END), 2) AS avg_stay_with_extra_baggage,
  ROUND(AVG(CASE WHEN wants_extra_baggage = 0 THEN length_of_stay ELSE NULL END), 2) AS avg_stay_without_extra_baggage,
  ROUND(AVG(wants_preferred_seat) * 100, 2) AS pct_preferred_seat,
  ROUND(AVG(wants_in_flight_meals) * 100, 2) AS pct_in_flight_meals,
  ROUND(AVG(wants_extra_baggage + wants_preferred_seat + wants_in_flight_meals), 2) AS avg_total_services
FROM
  stay_length_categories
GROUP BY
  stay_category
ORDER BY
  CASE
    WHEN stay_category = 'Short Stay (1-3 days)' THEN 1
    WHEN stay_category = 'Medium Stay (4-7 days)' THEN 2
    WHEN stay_category = 'Long Stay (8-14 days)' THEN 3
    ELSE 4
  END;

SELECT
  ROUND(CORR(length_of_stay, wants_extra_baggage), 4) AS correlation_stay_baggage
FROM
  `youtubescripts-425810.youtubebigquery.customerbooking`;





--D. PREDICTIVE ANALYTICS 
--1d. PREDICTING BOOKING COMPLETION d_rate DESC; 
-- PREPARE THE SAMPLE DATA 
WITH dataset AS (
  SELECT 
    num_passengers,
    sales_channel,
    trip_type,
    purchase_lead,
    length_of_stay,
    flight_hour,
    flight_day,
    route,
    booking_origin,
    wants_extra_baggage,
    wants_preferred_seat,
    wants_in_flight_meals,
    flight_duration,
    booking_complete
  FROM `youtubescripts-425810.youtubebigquery.customerbooking` 
),
split_data AS (
  SELECT
    *,
    FARM_FINGERPRINT(CONCAT(CAST(booking_complete AS STRING), CAST(RAND() AS STRING))) AS split_key,
    RANK() OVER (PARTITION BY booking_complete ORDER BY FARM_FINGERPRINT(CONCAT(CAST(booking_complete AS STRING), CAST(RAND() AS STRING)))) AS rank_key
  FROM dataset
)
SELECT
  num_passengers,
  sales_channel,
  trip_type,
  purchase_lead,
  length_of_stay,
  flight_hour,
  flight_day,
  route,
  booking_origin,
  wants_extra_baggage,
  wants_preferred_seat,
  wants_in_flight_meals,
  flight_duration,
  booking_complete,
  CASE
    WHEN rank_key <= 0.8 * COUNT(*) OVER (PARTITION BY booking_complete) THEN 'training'
    WHEN rank_key <= 0.9 * COUNT(*) OVER (PARTITION BY booking_complete) THEN 'evaluation'
    ELSE 'prediction'
  END AS dataframe
FROM split_data;




-- TRAIN THE MODEL 
-- Step 2: Train the Model
CREATE OR REPLACE MODEL `youtubescripts-425810.youtubebigquery.booking_model`
OPTIONS(
  model_type='logistic_reg',
  input_label_cols=['booking_complete'], 
  enable_global_explain=true
) AS
SELECT
  num_passengers,
  sales_channel,
  trip_type,
  purchase_lead,
  length_of_stay,
  flight_hour,
  flight_day,
  route,
  booking_origin,
  wants_extra_baggage,
  wants_preferred_seat,
  wants_in_flight_meals,
  flight_duration,
  booking_complete
FROM `youtubescripts-425810.youtubebigquery.datasplitmodel`
WHERE dataframe = 'training';





-- Evaluate the model
SELECT
  *
FROM
  ML.EVALUATE(MODEL `youtubescripts-425810.youtubebigquery.booking_model`,
    (
    SELECT
      *
    FROM
      `youtubescripts-425810.youtubebigquery.datasplitmodel`
    WHERE
      dataframe = 'evaluation'
    )
  );



-- Use the model to make predictions
SELECT
  *
FROM
  ML.PREDICT(MODEL `youtubescripts-425810.youtubebigquery.booking_model`,
    (
    SELECT
      *
    FROM
      `youtubescripts-425810.youtubebigquery.datasplitmodel`
    WHERE
      dataframe = 'prediction'
    )
  );



-- EXPLAIN PREDICTION RESULTS 
SELECT
  *
FROM
  ML.EXPLAIN_PREDICT(MODEL `youtubescripts-425810.youtubebigquery.booking_model`,
    (
    SELECT
      *
    FROM
      `youtubescripts-425810.youtubebigquery.datasplitmodel`
    WHERE
      dataframe = 'evaluation'
    ),
    STRUCT(3 AS top_k_features)
  );



  -- GLOBALLY EXPLAIN RESULTS 
  -- RETRAIN THE MODEL 

SELECT
  *
FROM
  ML.GLOBAL_EXPLAIN(MODEL `youtubescripts-425810.youtubebigquery.booking_model`)