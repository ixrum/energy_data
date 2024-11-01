# Energy Production Data Analysis Queries

The following document describes three SQL queries that are used to analyze energy production data in Germany. Each query has a specific purpose to support Business Intelligence (BI) and Machine Learning (ML) use cases by extracting and aggregating relevant data.

### Query 1: Trend of Daily Public Net Electricity Production in Germany
```sql
SELECT
    ep.timestamp,
    pt.production_type,
    SUM(ep.production_data) AS daily_net_production
FROM
    fact_energy_production ep
JOIN
    dim_production_type pt ON ep.production_type_id = pt.production_type_id
WHERE
    ep.country_name = "Germany"
GROUP BY
    t.date, pt.production_type
ORDER BY
    t.date, pt.production_type;
```
**Description**:
This query calculates the trend of daily net electricity production in Germany for each production type. The `fact_energy_production` table is joined with the `dim_production_type` table to associate the production data with specific energy types. The data is filtered for Germany and aggregated to compute the total daily production per production type, ordered by date and production type.

### Query 2: Analysis of Daily Price Against Net Power for Offshore and Onshore Wind
```sql
SELECT
    TO_DATE(timestamp) as daydate,
    pt.production_type,
    SUM(ep.production_data) AS daily_net_production,
    p.price
FROM
    fact_energy_production ep
JOIN
    dim_production_type pt ON ep.production_type_id = pt.production_type_id
JOIN
    price_data p ON TO_DATE(ep.timestamp) = TO_DATE(p.timestamp)
WHERE
    pt.production_type_name IN ('Wind onshore', 'Wind offshore')
GROUP BY
    pt.production_type
ORDER BY
    ep.timestamp;
```
**Description**:
This query is used to analyze the relationship between the daily price of electricity and the net production for both onshore and offshore wind power. It joins the `fact_energy_production` table with `dim_production_type` and `price_data` tables to gather the production type and price data for each day. The focus is on wind production types, which are filtered by the `production_type_name`. The results are aggregated to get the daily net production and matched with the corresponding electricity price.

### Query 3: Prediction of Underperformance on 30-Minute Intervals
```sql
SELECT
    ep.timestamp,
    pt.production_type,
    ep.production_data
FROM
    fact_energy_production ep
JOIN
    dim_production_type pt ON ep.production_type_id = pt.production_type_id
WHERE
    (MINUTE(ep.timestamp) % 30 = 0)  -- Filter for 30-minute intervals
ORDER BY
    ep.timestamp, pt.production_type;
```
**Description**:
This query extracts data for energy production at 30-minute intervals to aid in predicting underperformance. The `fact_energy_production` table is joined with the `dim_production_type` table to provide production type details. The `WHERE` clause filters the records to only include those where the timestamp falls on a 30-minute interval, providing a basis for the ML model to predict potential production shortfalls in these intervals.
