DROP TABLE IF EXISTS fact_table;
CREATE TABLE fact_table (
    trip_id BIGINT PRIMARY KEY,
    VendorID INT,
    datetime_id BIGINT,
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_and_fwd_flag NVARCHAR(1), CHECK (store_and_fwd_flag IN ('Y', 'N')),  
    pickup_location_id INT,
    dropoff_location_id INT,
    payment_type_id INT,
    fare_amount FLOAT,  -- Use FLOAT for approximate numeric data
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
);

DROP TABLE IF EXISTS payment_type_dim;
CREATE TABLE payment_type_dim (
    payment_type_id INT PRIMARY KEY,
    payment_type NVARCHAR(50)
);

DROP TABLE IF EXISTS rate_code_dim;
CREATE TABLE rate_code_dim (
    rate_code_id INT PRIMARY KEY,
    rate_code_name NVARCHAR(50)
);

DROP TABLE IF EXISTS datetime_dim;
CREATE TABLE datetime_dim (
    datetime_id BIGINT PRIMARY KEY,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    pick_hour INT,
    pick_day INT,
    pick_month INT,
    pick_year INT,
    pick_weekday INT
);
DROP TABLE IF EXISTS pickup_location_dim;
CREATE TABLE pickup_location_dim (
    pickup_location_id INT PRIMARY KEY,
    pickup_zone VARCHAR(100),
    pickup_latitude FLOAT,
    pickup_longitude FLOAT,
    pickup_borough VARCHAR(100)
);

DROP TABLE IF EXISTS dropoff_location_dim;
CREATE TABLE dropoff_location_dim (
    dropoff_location_id INT PRIMARY KEY,
    dropoff_zone VARCHAR(100),
    dropoff_latitude FLOAT,
    dropoff_longitute FLOAT,
    dropoff_borough VARCHAR(100)
);

DROP TABLE IF EXISTS dbo.NYC_Taxi_Data_Unified ; 
CREATE TABLE dbo.NYC_Taxi_Data_Unified (
    trip_id BIGINT,
    VendorID INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count FLOAT,
    trip_distance FLOAT,
    rate_code_name NVARCHAR(50),
    pickup_latitude FLOAT,
    pickup_longitude FLOAT,
    pickup_zone VARCHAR(100),
    pickup_borough VARCHAR(100),
    dropoff_latitude FLOAT,
    dropoff_longitude FLOAT,
    dropoff_zone NVARCHAR(100),
    dropoff_borough VARCHAR(100),
    payment_type NVARCHAR(50),
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
);
INSERT INTO dbo.NYC_Taxi_Data_Unified
SELECT  
f.trip_id,
f.VendorID,
dt.tpep_pickup_datetime,
dt.tpep_dropoff_datetime,
f.passenger_count,
f.trip_distance,
r.rate_code_name,
pick.pickup_latitude,
pick.pickup_longitude,
pick.pickup_zone,
pick.pickup_borough,
dp.dropoff_latitude,
dp.dropoff_longitude,
dp.dropoff_zone,
dp.dropoff_borough,
pay.payment_type,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM dbo.fact_table f
LEFT JOIN dbo.payment_type_dim pay ON pay.payment_type_id = f.payment_type_id
LEFT JOIN dbo.rate_code_dim r ON r.rate_code_id = f.rate_code_id
LEFT JOIN dbo.datetime_dim dt ON dt.datetime_id = f.datetime_id
LEFT JOIN dbo.pickup_location_dim pick ON pick.pickup_location_id = f.pickup_location_id
LEFT JOIN dbo.dropoff_location_dim dp ON dp.dropoff_location_id = f.dropoff_location_id
;