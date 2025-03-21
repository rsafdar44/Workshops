-- Setup: Snowflake Trial Account with ACCOUNTADMIN access: https://signup.snowflake.com/?trial=student&cloud=aws&region=us-west-2&utm_source=hol&utm_campaign=northstar-hols-2025
-- Reference this github repo: https://github.com/Snowflake-Labs/modern-data-engineering-snowflake

-- Ingest through the Snowflake UI 
-- Load data from the Marketplace: “Weather Source LLC: frostbyte” – first result 
-- Options drop down
-- Rename the dataset: “FROSTBYTE_WEATHERSOURCE”
-- Click Query data to open a new SQL worksheet
-- Open new Worksheet
-- Hover over ONPOINT_ID schema 
-- Click 3 dots -> “set worksheet context” to let the worksheet know which dataset we plan to run queries against 
-- From the Github repo: frostbyte-weathersource.sql file to return the average temperature and total precipitation for cities in France. Copy/paste into worksheet and click “run all”
SELECT
  city_name,
  country,
  AVG(avg_temperature_air_2m_f) AS avg_temperature,
  SUM(tot_precipitation_in) AS total_precipitation
FROM
  history_day
WHERE
  date_valid_std >= DATEADD (DAY, -7, CURRENT_DATE) 
  AND country = 'FR'
GROUP BY
  city_name,
  country
ORDER BY
  total_precipitation DESC;

-- [PLACEHOLDER FOR COPY INTO EXERCISE] 

-- Data Transformations
-- Create a new SQL worksheet 
-- Select the Hamburg_sales.sql file from the repo, copy/paste into the worksheet DO NOT RUN ALL 
-- Run first 3 lines 
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;
-- Create a view that adds weather data to the cities where Tasty Bytes operates
CREATE OR REPLACE VIEW tasty_bytes.harmonized.daily_weather_v
COMMENT = 'Weather Source Daily History filtered to Tasty Bytes supported Cities'
    AS
SELECT
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM FROSTBYTE_WEATHERSOURCE.onpoint_id.history_day hd
JOIN FROSTBYTE_WEATHERSOURCE.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN TASTY_BYTES.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;
-- Run second block of SQL to create views daily_weather_v and harmonized schema 
CREATE OR REPLACE VIEW tasty_bytes.harmonized.daily_weather_v
COMMENT = 'Weather Source Daily History filtered to Tasty Bytes supported Cities'
    AS
SELECT
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM FROSTBYTE_WEATHERSOURCE.onpoint_id.history_day hd
JOIN FROSTBYTE_WEATHERSOURCE.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN TASTY_BYTES.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;
-- Run the final block of SQL code to return wind speed of those dates 
CREATE OR REPLACE VIEW tasty_bytes.harmonized.windspeed_hamburg
    AS
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc IN ('Germany')
    AND dw.city_name = 'Hamburg'
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;
-- Click “chart” to see the visualization 

-- User Defined Functions
-- Open udf_temp_length.sql file in the repo. Copy/paste into new SQL worksheet
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

CREATE OR REPLACE FUNCTION tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
  RETURNS NUMBER(35,4)
  AS
  $$
    (temp_f - 32) * (5/9)
  $$
;

CREATE OR REPLACE FUNCTION tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))
  RETURNS NUMBER(35,4)
  AS
  $$
    inch * 25.4
  $$
;
-- Open hamburg_sales_expanded.sql. Copy/paste into new SQL worksheet 
-- The First block returns wind speeds with new columns and returns temperature in Fahrenheit and precipitation in inches 
-- Run this query again from the sql file 
CREATE OR REPLACE VIEW harmonized.weather_hamburg
AS
SELECT
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM harmonized.daily_weather_v fd
LEFT JOIN harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
    AND fd.country_desc = 'Germany'
    AND fd.city = 'Hamburg'
    AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
ORDER BY fd.date_valid_std ASC;
-- Run last block of statements to show the view was created on the left in the ANALYTICS schema 
CREATE OR REPLACE VIEW analytics.daily_city_metrics_v
COMMENT = 'Daily Weather Metrics and Orders Data'
AS
SELECT
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;

--Stored Procedures 
-- Open the Orders_header_sproc.sql file and copy/paste everything into a new SQL worksheet. Do not run all yet.  
-- The stored procedure will process the order_headers_stream that we created earlier that tracks changes to the orders_header table
-- Run SQL statements that set context and block of SQL that creates the stored procedure
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

-- Create the stored procedure, define its logic with Snowpark for Python, write sales to raw_pos.daily_sales_hamburg_t
CREATE OR REPLACE PROCEDURE tasty_bytes.raw_pos.process_order_headers_stream()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER ='process_order_headers_stream'
  PACKAGES = ('snowflake-snowpark-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session

def process_order_headers_stream(session: Session) -> float:
    # Query the stream
    recent_orders = session.table("order_header_stream").filter(F.col("METADATA$ACTION") == "INSERT")
    
    # Look up location of the orders in the stream using the LOCATIONS table
    locations = session.table("location")
    hamburg_orders = recent_orders.join(
        locations,
        recent_orders["LOCATION_ID"] == locations["LOCATION_ID"]
    ).filter(
        (locations["CITY"] == "Hamburg") &
        (locations["COUNTRY"] == "Germany")
    )
    
    # Calculate the sum of sales in Hamburg
    total_sales = hamburg_orders.group_by(F.date_trunc('DAY', F.col("ORDER_TS"))).agg(
        F.coalesce(F.sum("ORDER_TOTAL"), F.lit(0)).alias("total_sales")
    )
    
    # Select the columns with proper aliases and convert to date type
    daily_sales = total_sales.select(
        F.col("DATE_TRUNC('DAY', ORDER_TS)").cast("DATE").alias("DATE"),
        F.col("total_sales")
    )
    
    # Write the results to the DAILY_SALES_HAMBURG_T table
    total_sales.write.mode("append").save_as_table("raw_pos.daily_sales_hamburg_t")
    
    # Return a message indicating the operation was successful
    return "Daily sales for Hamburg, Germany have been successfully written to raw_pos.daily_sales_hamburg_t"
$$;
-- Run the INSERT INTO statement to insert dummy data
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, 
    TRUCK_ID, 
    LOCATION_ID, 
    CUSTOMER_ID, 
    DISCOUNT_ID, 
    SHIFT_ID, 
    SHIFT_START_TIME, 
    SHIFT_END_TIME, 
    ORDER_CHANNEL, 
    ORDER_TS, 
    SERVED_TS, 
    ORDER_CURRENCY, 
    ORDER_AMOUNT, 
    ORDER_TAX_AMOUNT, 
    ORDER_DISCOUNT_AMOUNT, 
    ORDER_TOTAL
) VALUES (
    123456789,                     -- ORDER_ID
    101,                           -- TRUCK_ID
    4493,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2023-07-01 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    41.30,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    45.80                          -- ORDER_TOTAL
);
-- Run the SELECT statement to verify the insert and call the stored procedure
-- Confirm the insert
SELECT * FROM tasty_bytes.raw_pos.order_header WHERE location_id = 4493;
-- Call the stored procedure
CALL tasty_bytes.raw_pos.process_order_headers_stream();
-- Confirm the insert to the daily_sales_hamburg_t table
SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg_t;

-- Data Delivery with Streamlit
-- Navigate to Snowsight and Create a Streamlit App
-- Name the app: WAGES_CPI_USA_APP
-- App location: WAGES_CPI db
-- select the DATA schema
-- select the COMPUTE_WH 
-- Open the streamlit.py file and copy/paste into the code editor for streamlit. Click run 
-- Now you have an app that visualizes the CPI on a monthly basis and average annual wages and CPI on a yearly basis


