

CREATE TABLE IF NOT EXISTS "2024_juan_franco_torrez_schema".dim_asteroids
(
	 asteroid_id int   ENCODE lzo
	,asteroid_name VARCHAR(256)   ENCODE lzo
	,absolute_magnitude_h DOUBLE PRECISION   ENCODE RAW
	,min_estimate_diameter_km DOUBLE PRECISION   ENCODE RAW
	,max_estimate_diameter_km DOUBLE PRECISION   ENCODE RAW

)
DISTSTYLE AUTO
;
ALTER table "2024_juan_franco_torrez_schema".dim_asteroids owner to "2024_juan_franco_torrez";



CREATE TABLE IF NOT EXISTS "2024_juan_franco_torrez_schema".fact_asteroidsnearearth
(
	date datetime
	,asteroid_id int   ENCODE lzo
	,is_potentially_hazardous_asteroid BOOLEAN   ENCODE RAW
	,velocity_km_sec DOUBLE PRECISION   ENCODE RAW
	,miss_lunar_distance DOUBLE PRECISION   ENCODE RAW
	,miss_km_distance DOUBLE PRECISION   ENCODE RAW
	,miss_astronomical_distance DOUBLE PRECISION   ENCODE RAW
)
DISTSTYLE AUTO
;
ALTER TABLE "2024_juan_franco_torrez_schema".fact_asteroidsnearearth owner to "2024_juan_franco_torrez";

--select * from "2024_juan_franco_torrez_schema".dim_asteroids
-- select * from "2024_juan_franco_torrez_schema".fact_asteroidsnearearth

-- truncate table "2024_juan_franco_torrez_schema".dim_asteroids