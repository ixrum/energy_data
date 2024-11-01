SELECT 
    ep.timestamp
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

