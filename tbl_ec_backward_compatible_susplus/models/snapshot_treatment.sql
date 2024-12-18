{{ config(
    materialized='incremental'
	,pre_hook="delete from {{ this }} where source_table= 'tbl_ec_backward_compatible_susplus'"
    ) }}

with source_data as (

SELECT 
person_id,
digest,
"START_DATE",
START_TIME,
TREATMENT_CODE_ORDER,
TREATMENT_CODE,
ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,digest,"START_DATE",START_TIME
		ORDER BY 
		(CASE 
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - First' THEN 1
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Second' THEN 2
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Third' THEN 3
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Fourth' THEN 4
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Fifth' THEN 5
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Sixth' THEN 6
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Seventh' THEN 7
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Eighth' THEN 8
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Ninth' THEN 9
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Tenth' THEN 10
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Eleventh' THEN 11
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Twelfth' THEN 12
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Thirteenth' THEN 13
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Fourteenth' THEN 14
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Fifteenth' THEN 15
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Sixteenth' THEN 16
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Seventeenth' THEN 17
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Eighteenth' THEN 18
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Nineteenth' THEN 19
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Twentieth' THEN 20
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Twenty First' THEN 21
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Twenty Second' THEN 22
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Twenty Third' THEN 23
			WHEN TREATMENT_CODE_ORDER='Accident And Emergency Treatment - Twenty Fourth' THEN 24
			END ) 
		) AS TREATMENT_CODE_SEQ

    FROM
        (
        select 
        person_id PERSON_ID,
		digest,
        [Arrival Date] AS "START_DATE",
        [Arrival Time] AS START_TIME,
        [Accident And Emergency Treatment - First]	,
		[Accident And Emergency Treatment - Second]	,
		[Accident And Emergency Treatment - Third]	,
		[Accident And Emergency Treatment - Fourth]	,
		[Accident And Emergency Treatment - Fifth]	,
		[Accident And Emergency Treatment - Sixth]	,
		[Accident And Emergency Treatment - Seventh]	,
		[Accident And Emergency Treatment - Eighth]	,
		[Accident And Emergency Treatment - Ninth]	,
		[Accident And Emergency Treatment - Tenth]	,
		[Accident And Emergency Treatment - Eleventh]	,
		[Accident And Emergency Treatment - Twelfth]	,
		[Accident And Emergency Treatment - Thirteenth]	,
		[Accident And Emergency Treatment - Fourteenth]	,
		[Accident And Emergency Treatment - Fifteenth]	,
		[Accident And Emergency Treatment - Sixteenth]	,
		[Accident And Emergency Treatment - Seventeenth]	,
		[Accident And Emergency Treatment - Eighteenth]	,
		[Accident And Emergency Treatment - Nineteenth]	,
		[Accident And Emergency Treatment - Twentieth]	,
		[Accident And Emergency Treatment - Twenty First]	,
		[Accident And Emergency Treatment - Twenty Second]	,
		[Accident And Emergency Treatment - Twenty Third]	,
		[Accident And Emergency Treatment - Twenty Fourth]	

        from 
        --tbl_ec_backward_compatible_susplus
		{{ source('warehouse', 'tbl_ec_backward_compatible_susplus') }}
		WHERE digest IS NOT NULL
        ) As pv
        unpivot
        (
        TREATMENT_CODE FOR TREATMENT_CODE_ORDER IN 
            (
			[Accident And Emergency Treatment - First]	,
			[Accident And Emergency Treatment - Second]	,
			[Accident And Emergency Treatment - Third]	,
			[Accident And Emergency Treatment - Fourth]	,
			[Accident And Emergency Treatment - Fifth]	,
			[Accident And Emergency Treatment - Sixth]	,
			[Accident And Emergency Treatment - Seventh]	,
			[Accident And Emergency Treatment - Eighth]	,
			[Accident And Emergency Treatment - Ninth]	,
			[Accident And Emergency Treatment - Tenth]	,
			[Accident And Emergency Treatment - Eleventh]	,
			[Accident And Emergency Treatment - Twelfth]	,
			[Accident And Emergency Treatment - Thirteenth]	,
			[Accident And Emergency Treatment - Fourteenth]	,
			[Accident And Emergency Treatment - Fifteenth]	,
			[Accident And Emergency Treatment - Sixteenth]	,
			[Accident And Emergency Treatment - Seventeenth]	,
			[Accident And Emergency Treatment - Eighteenth]	,
			[Accident And Emergency Treatment - Nineteenth]	,
			[Accident And Emergency Treatment - Twentieth]	,
			[Accident And Emergency Treatment - Twenty First]	,
			[Accident And Emergency Treatment - Twenty Second]	,
			[Accident And Emergency Treatment - Twenty Third]	,
			[Accident And Emergency Treatment - Twenty Fourth]	
            )
        ) AS UNPVT
    WHERE TREATMENT_CODE<>''
)
SELECT 
PERSON_ID,
digest,
"START_DATE",
START_TIME,
cast(null as varchar) as END_DATE,
cast(null as varchar) as END_TIME,
TREATMENT_CODE,
TREATMENT_CODE_ORDER,
TREATMENT_CODE_SEQ,
'tbl_ec_backward_compatible_susplus' as SOURCE_TABLE
FROM   
source_data
;