
---------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE ip_stat_dt
TARGET_LAG = '1 minute'
WAREHOUSE = analysis_wh
REFRESH_MODE = INCREMENTAL
AS
select "ip", MAX(TO_TIMESTAMP("timestamp" / 1e9)) AS latest from my_events group by all;

select * from ip_stat_dt;

-----------------------                        -----------------------------

CREATE OR REPLACE STREAM new_events ON TABLE my_events;


CREATE OR REPLACE TABLE ip_stat_tb as select "ip", MAX(TO_TIMESTAMP("timestamp" / 1e9)) AS latest from my_events group by all;


CREATE OR REPLACE TASK ip_stat_task
WAREHOUSE = analysis_wh
SCHEDULE = '5 minute' --'USING CRON 0 * * * * UTC' 
AS
MERGE INTO ip_stat_tb tgt
USING (
    SELECT
    "ip", MAX(TO_TIMESTAMP("timestamp" / 1e9)) AS latest
    FROM new_events
    GROUP BY all
) src
ON tgt."ip" = src."ip"
WHEN MATCHED THEN UPDATE SET
    LATEST = src.LATEST
WHEN NOT MATCHED THEN INSERT (
    "ip" , latest
) VALUES (
    src."ip", src.LATEST
);

alter task ip_stat_task resume;