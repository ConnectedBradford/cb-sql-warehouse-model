{{ config(
    materialized='incremental'
	,pre_hook="delete from {{ this }} where source_table= 'tbl_ae'" 
    ) }}

with source_data as (

SELECT 
PERSON_ID,
digest,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
INVESTIGATION_CODE_ORDER,
INVESTIGATION_CODE,
ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,digest,"START_DATE",START_TIME
		ORDER BY 
		(CASE 
			WHEN INVESTIGATION_CODE_ORDER='investigation_1' THEN 1
			WHEN INVESTIGATION_CODE_ORDER='investigation_2' THEN 2
			WHEN INVESTIGATION_CODE_ORDER='investigation_3' THEN 3
			WHEN INVESTIGATION_CODE_ORDER='investigation_4' THEN 4
			WHEN INVESTIGATION_CODE_ORDER='investigation_5' THEN 5
			WHEN INVESTIGATION_CODE_ORDER='investigation_6' THEN 6
			WHEN INVESTIGATION_CODE_ORDER='investigation_7' THEN 7
			WHEN INVESTIGATION_CODE_ORDER='investigation_8' THEN 8
			WHEN INVESTIGATION_CODE_ORDER='investigation_9' THEN 9
			WHEN INVESTIGATION_CODE_ORDER='investigation_10' THEN 10
			WHEN INVESTIGATION_CODE_ORDER='investigation_11' THEN 11
			WHEN INVESTIGATION_CODE_ORDER='investigation_12' THEN 12
		END ) 
		) AS INVESTIGATION_CODE_SEQ,
---
ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,digest,"START_DATE",START_TIME,END_DATE,END_TIME,INVESTIGATION_CODE
		ORDER BY "START_DATE") as cnt

    FROM
        (
        select 
			person_id,
			digest,
			investigation_1,
			investigation_2,
			investigation_3,
			investigation_4,
			investigation_5,
			investigation_6,
			investigation_7,
			investigation_8,
			investigation_9,
			investigation_10,
			investigation_11,
			investigation_12,
			CONVERT(varchar,arrival_date_time,112) start_date,
			CONVERT(VARCHAR, arrival_date_time, 108) start_time,
			CONVERT(varchar,departure_date_time,112) end_date,
			CONVERT(VARCHAR, departure_date_time, 108) end_time
			from 
			--tbl_ae
			{{ source('warehouse', 'tbl_ae') }}
			WHERE digest IS NOT NULL 
			) As pv
        unpivot
        (
        INVESTIGATION_CODE FOR INVESTIGATION_CODE_ORDER IN 
            (
                investigation_1,
				investigation_2,
				investigation_3,
				investigation_4,
				investigation_5,
				investigation_6,
				investigation_7,
				investigation_8,
				investigation_9,
				investigation_10,
				investigation_11,
				investigation_12	
            )
        ) AS UNPVT
    WHERE INVESTIGATION_CODE<>''
)
SELECT 
PERSON_ID,
digest,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
INVESTIGATION_CODE,
INVESTIGATION_CODE_ORDER,
INVESTIGATION_CODE_SEQ,cnt,
'tbl_ae' as SOURCE_TABLE
FROM   
source_data
where cnt=1
;