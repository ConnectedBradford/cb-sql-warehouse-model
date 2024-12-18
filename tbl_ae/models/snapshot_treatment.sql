{{ config(
    materialized='incremental'
	,pre_hook="delete from {{ this }} where source_table= 'tbl_ae'" 
    ) }}

with source_data as (

SELECT 
person_id,
digest,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
TREATMENT_CODE_ORDER,
TREATMENT_CODE,
ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,digest,"START_DATE",START_TIME
		ORDER BY 
		(CASE 
			WHEN TREATMENT_CODE_ORDER='treatment_1' THEN 1
			WHEN TREATMENT_CODE_ORDER='treatment_2' THEN 2
			WHEN TREATMENT_CODE_ORDER='treatment_3' THEN 3
			WHEN TREATMENT_CODE_ORDER='treatment_4' THEN 4
			WHEN TREATMENT_CODE_ORDER='treatment_5' THEN 5
			WHEN TREATMENT_CODE_ORDER='treatment_6' THEN 6
			WHEN TREATMENT_CODE_ORDER='treatment_7' THEN 7
			WHEN TREATMENT_CODE_ORDER='treatment_8' THEN 8
			WHEN TREATMENT_CODE_ORDER='treatment_9' THEN 9
			WHEN TREATMENT_CODE_ORDER='treatment_10' THEN 10
			WHEN TREATMENT_CODE_ORDER='treatment_11' THEN 11
			WHEN TREATMENT_CODE_ORDER='treatment_12' THEN 12
			END ) 
		) AS TREATMENT_CODE_SEQ,
	ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,digest,"START_DATE",START_TIME,END_DATE,END_TIME,TREATMENT_CODE
		ORDER BY "START_DATE") as cnt

    FROM
        (
        select 
        person_id,
		digest,
		treatment_1,
		treatment_2,
		treatment_3,
		treatment_4,
		treatment_5,
		treatment_6,
		treatment_7,
		treatment_8,
		treatment_9,
		treatment_10,
		treatment_11,
		treatment_12,
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
        TREATMENT_CODE FOR TREATMENT_CODE_ORDER IN 
            (
			treatment_1,
			treatment_2,
			treatment_3,
			treatment_4,
			treatment_5,
			treatment_6,
			treatment_7,
			treatment_8,
			treatment_9,
			treatment_10,
			treatment_11,
			treatment_12	
            )
        ) AS UNPVT
    WHERE TREATMENT_CODE<>''
)
SELECT 
 
PERSON_ID,
digest,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
TREATMENT_CODE,
TREATMENT_CODE_ORDER,
TREATMENT_CODE_SEQ,
'tbl_ae' as SOURCE_TABLE
FROM   
source_data
where cnt=1---Remove duplicate tretment_code
;