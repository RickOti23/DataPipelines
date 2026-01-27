SELECT COUNT(*) AS short_trips
FROM public.green_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;


SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS longest_trip_distance
FROM public.green_trips
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY longest_trip_distance DESC
LIMIT 1;
