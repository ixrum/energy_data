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

