COPY {{ params.schema }}.{{ params.table }}
FROM s3://{{ params.s3_bucket }}/{{ params.s3_key }}/{{ params.file_name }}
with credentials
aws_access_key_id={{ params.access_key }};aws_secret_access_key={{ params.secret_key }}
CSV;

-- Upsert cpi table
UPDATE movies.cpi 
SET consumer_price_index = sc.consumer_price_index
FROM movies.stage_cpi sc
WHERE movies.cpi.date_cd= sc.date_cd; 


INSERT INTO movies.cpi
SELECT sc.* FROM movies.stage_cpi sc LEFT JOIN movies.cpi 
ON sc.date_cd = movies.cpi.date_cd
WHERE movies.cpi.date_cd IS NULL;