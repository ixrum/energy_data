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
