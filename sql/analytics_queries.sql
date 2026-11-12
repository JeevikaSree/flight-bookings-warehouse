-- Business questions on the flight bookings warehouse
USE CATALOG workspace;
USE SCHEMA flights;

-- 1. Revenue and bookings by airline (excluding cancellations)
SELECT fl.airline, COUNT(*) AS bookings, ROUND(SUM(f.fare), 2) AS revenue
FROM fact_bookings f
JOIN dim_flight fl ON f.flight_id = fl.flight_id
WHERE f.status <> 'CANCELLED'
GROUP BY fl.airline
ORDER BY revenue DESC;

-- 2. Busiest routes
SELECT fl.origin, fl.destination, COUNT(*) AS bookings
FROM fact_bookings f
JOIN dim_flight fl ON f.flight_id = fl.flight_id
GROUP BY fl.origin, fl.destination
ORDER BY bookings DESC
LIMIT 10;

-- 3. Revenue by customer city AS IT WAS when they booked (uses the SCD2 version via customer_sk)
SELECT c.city, ROUND(SUM(f.fare), 2) AS revenue
FROM fact_bookings f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.city
ORDER BY revenue DESC;

-- 4. Same question using the customer's CURRENT city (compare with query 3)
SELECT c.city, ROUND(SUM(f.fare), 2) AS revenue
FROM fact_bookings f
JOIN dim_customer c ON f.customer_id = c.customer_id AND c.is_current
GROUP BY c.city
ORDER BY revenue DESC;

-- 5. Cancellation rate by cabin
SELECT cabin,
       ROUND(AVG(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) * 100, 1) AS cancel_pct
FROM fact_bookings
GROUP BY cabin;

-- 6. Delta Lake time travel: the fact table before the day-2 load
DESCRIBE HISTORY fact_bookings;
-- SELECT COUNT(*) FROM fact_bookings VERSION AS OF 1;
