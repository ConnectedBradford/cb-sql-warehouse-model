{{ config(
    materialized='incremental'
    ) }}

with source_data as (

SELECT 
person_id,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
DIAG_CODE_ORDER,
DIAG_CODE,
ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,"START_DATE",START_TIME,END_DATE,
		END_TIME
		ORDER BY 
		(CASE 
			WHEN DIAG_CODE_ORDER='Diagnosis Primary (ICD)' THEN 1
			WHEN DIAG_CODE_ORDER='Diagnosis 1st Secondary (ICD)' THEN 2
			WHEN DIAG_CODE_ORDER='Diagnosis 2nd Secondary (ICD)' THEN 3
			WHEN DIAG_CODE_ORDER='Diagnosis 3rd Secondary (ICD)' THEN 4
			WHEN DIAG_CODE_ORDER='Diagnosis 4th Secondary (ICD)' THEN 5
			WHEN DIAG_CODE_ORDER='Diagnosis 5th Secondary (ICD)' THEN 6
			WHEN DIAG_CODE_ORDER='Diagnosis 6th Secondary (ICD)' THEN 7
			WHEN DIAG_CODE_ORDER='Diagnosis 7th Secondary (ICD)' THEN 8
			WHEN DIAG_CODE_ORDER='Diagnosis 8th Secondary (ICD)' THEN 9
			WHEN DIAG_CODE_ORDER='Diagnosis 9th Secondary (ICD)' THEN 10
			WHEN DIAG_CODE_ORDER='Diagnosis 10th Secondary (ICD)' THEN 11
			WHEN DIAG_CODE_ORDER='Diagnosis 11th Secondary (ICD)' THEN 12
			WHEN DIAG_CODE_ORDER='Diagnosis 12th Secondary (ICD)' THEN 13
			WHEN DIAG_CODE_ORDER='Diagnosis 13th Secondary (ICD)' THEN 14
			WHEN DIAG_CODE_ORDER='Diagnosis 14th Secondary (ICD)' THEN 15
			WHEN DIAG_CODE_ORDER='Diagnosis 15th Secondary (ICD)' THEN 16
			WHEN DIAG_CODE_ORDER='Diagnosis 16th Secondary (ICD)' THEN 17
			WHEN DIAG_CODE_ORDER='Diagnosis 17th Secondary (ICD)' THEN 18
			WHEN DIAG_CODE_ORDER='Diagnosis 18th Secondary (ICD)' THEN 19
			WHEN DIAG_CODE_ORDER='Diagnosis 19th Secondary (ICD)' THEN 20
			WHEN DIAG_CODE_ORDER='Diagnosis 20th Secondary (ICD)' THEN 21
			WHEN DIAG_CODE_ORDER='Diagnosis 21st Secondary (ICD)' THEN 22
			WHEN DIAG_CODE_ORDER='Diagnosis 22nd Secondary (ICD)' THEN 23
			WHEN DIAG_CODE_ORDER='Diagnosis 23rd Secondary (ICD)' THEN 24
			END ) 
		) AS DIAG_CODE_SEQ

    FROM
        (
        select 
        person_id,
        [Appointment Date] AS "START_DATE",
        [Appointment Time] AS START_TIME,
        cast(null as varchar) AS END_DATE,
        cast(null as varchar) AS END_TIME,
        --[Diagnosis Scheme In Use (ICD)],
        [Diagnosis Primary (ICD)],
        [Diagnosis 1st Secondary (ICD)],
        [Diagnosis 2nd Secondary (ICD)],
        [Diagnosis 3rd Secondary (ICD)],
        [Diagnosis 4th Secondary (ICD)],
        [Diagnosis 5th Secondary (ICD)],
        [Diagnosis 6th Secondary (ICD)],
        [Diagnosis 7th Secondary (ICD)],
        [Diagnosis 8th Secondary (ICD)],
        [Diagnosis 9th Secondary (ICD)],
        [Diagnosis 10th Secondary (ICD)],
        [Diagnosis 11th Secondary (ICD)],
        [Diagnosis 12th Secondary (ICD)],
        [Diagnosis 13th Secondary (ICD)],
        [Diagnosis 14th Secondary (ICD)],
        [Diagnosis 15th Secondary (ICD)],
        [Diagnosis 16th Secondary (ICD)],
        [Diagnosis 17th Secondary (ICD)],
        [Diagnosis 18th Secondary (ICD)],
        [Diagnosis 19th Secondary (ICD)],
        [Diagnosis 20th Secondary (ICD)],
        [Diagnosis 21st Secondary (ICD)],
        [Diagnosis 22nd Secondary (ICD)],
        [Diagnosis 23rd Secondary (ICD)]

        from 
        --[dbo].[tbl_op_susplus]
		{{ source('warehouse', 'tbl_op_susplus') }}
        where 
        [Diagnosis Scheme In Use (ICD)] is not null---This value for valid records are 02
        and  [Diagnosis Scheme In Use (ICD)]<>''
		--and person_id=10646924
        ) As pv
        unpivot
        (
        DIAG_CODE FOR DIAG_CODE_ORDER IN 
            (
                [Diagnosis Primary (ICD)],
                [Diagnosis 1st Secondary (ICD)],
                [Diagnosis 2nd Secondary (ICD)],
                [Diagnosis 3rd Secondary (ICD)],
                [Diagnosis 4th Secondary (ICD)],
                [Diagnosis 5th Secondary (ICD)],
                [Diagnosis 6th Secondary (ICD)],
                [Diagnosis 7th Secondary (ICD)],
                [Diagnosis 8th Secondary (ICD)],
                [Diagnosis 9th Secondary (ICD)],
                [Diagnosis 10th Secondary (ICD)],
                [Diagnosis 11th Secondary (ICD)],
                [Diagnosis 12th Secondary (ICD)],
                [Diagnosis 13th Secondary (ICD)],
                [Diagnosis 14th Secondary (ICD)],
                [Diagnosis 15th Secondary (ICD)],
                [Diagnosis 16th Secondary (ICD)],
                [Diagnosis 17th Secondary (ICD)],
                [Diagnosis 18th Secondary (ICD)],
                [Diagnosis 19th Secondary (ICD)],
                [Diagnosis 20th Secondary (ICD)],
                [Diagnosis 21st Secondary (ICD)],
                [Diagnosis 22nd Secondary (ICD)],
                [Diagnosis 23rd Secondary (ICD)]
            )
        ) AS UNPVT
    WHERE DIAG_CODE<>''
)
SELECT 
person_id,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
DIAG_CODE_ORDER,
DIAG_CODE,
DIAG_CODE_SEQ,
'tbl_op_susplus' as source_table
FROM   
source_data
