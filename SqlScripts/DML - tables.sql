

CREATE TABLE IF NOT EXISTS "2024_juan_franco_torrez_schema".asteroids
(
	 asteroid_id VARCHAR(256)   ENCODE lzo
	,asteroid_name VARCHAR(256)   ENCODE lzo
	,absolute_magnitude_h DOUBLE PRECISION   ENCODE RAW
	,min_estimate_diameter_km DOUBLE PRECISION   ENCODE RAW
	,max_estimate_diameter_km DOUBLE PRECISION   ENCODE RAW

)
DISTSTYLE AUTO
;
ALTER TABLE "2024_juan_franco_torrez_schema".asteroids owner to "2024_juan_franco_torrez";



CREATE TABLE IF NOT EXISTS "2024_juan_franco_torrez_schema".asteroidsnearearth
(
	date VARCHAR(256)   ENCODE lzo
	,asteroid_id VARCHAR(256)   ENCODE lzo
	,is_potentially_hazardous_asteroid BOOLEAN   ENCODE RAW
	,velocity_km_sec VARCHAR(256)   ENCODE lzo
	,miss_lunar_distance VARCHAR(256)   ENCODE lzo
	,miss_km_distance VARCHAR(256)   ENCODE lzo
	,miss_astronomical_distance VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE "2024_juan_franco_torrez_schema".asteroidsnearearth owner to "2024_juan_franco_torrez";

--select * from "2024_juan_franco_torrez_schema".asteroids
-- select * from "2024_juan_franco_torrez_schema".asteroidsnearearth