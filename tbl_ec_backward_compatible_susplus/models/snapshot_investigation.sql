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
INVESTIGATION_CODE_ORDER,
INVESTIGATION_CODE,
ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,digest,"START_DATE",START_TIME
		ORDER BY 
		(CASE 
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - First' THEN 1
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Second' THEN 2
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Third' THEN 3
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Fourth' THEN 4
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Fifth' THEN 5
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Sixth' THEN 6
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Seventh' THEN 7
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Eighth' THEN 8
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Ninth' THEN 9
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Tenth' THEN 10
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Eleventh' THEN 11
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Twelfth' THEN 12
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Thirteenth' THEN 13
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Fourteenth' THEN 14
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Fifteenth' THEN 15
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Sixteenth' THEN 16
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Seventeenth' THEN 17
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Eighteenth' THEN 18
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Nineteenth' THEN 19
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Twentieth' THEN 20
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Twenty First' THEN 21
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Twenty Second' THEN 22
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Twenty Third' THEN 23
			WHEN INVESTIGATION_CODE_ORDER='Accident And Emergency Investigation - Twenty Fourth' THEN 24
			END ) 
		) AS INVESTIGATION_CODE_SEQ

    FROM
        (
        select 
        person_id PERSON_ID,
		digest,
        [Arrival Date] AS "START_DATE",
        [Arrival Time] AS START_TIME,
        [Accident And Emergency Investigation - First]	,
		[Accident And Emergency Investigation - Second]	,
		[Accident And Emergency Investigation - Third]	,
		[Accident And Emergency Investigation - Fourth]	,
		[Accident And Emergency Investigation - Fifth]	,
		[Accident And Emergency Investigation - Sixth]	,
		[Accident And Emergency Investigation - Seventh]	,
		[Accident And Emergency Investigation - Eighth]	,
		[Accident And Emergency Investigation - Ninth]	,
		[Accident And Emergency Investigation - Tenth]	,
		[Accident And Emergency Investigation - Eleventh]	,
		[Accident And Emergency Investigation - Twelfth]	,
		[Accident And Emergency Investigation - Thirteenth]	,
		[Accident And Emergency Investigation - Fourteenth]	,
		[Accident And Emergency Investigation - Fifteenth]	,
		[Accident And Emergency Investigation - Sixteenth]	,
		[Accident And Emergency Investigation - Seventeenth]	,
		[Accident And Emergency Investigation - Eighteenth]	,
		[Accident And Emergency Investigation - Nineteenth]	,
		[Accident And Emergency Investigation - Twentieth]	,
		[Accident And Emergency Investigation - Twenty First]	,
		[Accident And Emergency Investigation - Twenty Second]	,
		[Accident And Emergency Investigation - Twenty Third]	,
		[Accident And Emergency Investigation - Twenty Fourth]	


        from 
        --tbl_ec_backward_compatible_susplus
		{{ source('warehouse', 'tbl_ec_backward_compatible_susplus') }}
		WHERE digest IS NOT NULL
        ) As pv
        unpivot
        (
        INVESTIGATION_CODE FOR INVESTIGATION_CODE_ORDER IN 
            (
                [Accident And Emergency Investigation - First]	,
				[Accident And Emergency Investigation - Second]	,
				[Accident And Emergency Investigation - Third]	,
				[Accident And Emergency Investigation - Fourth]	,
				[Accident And Emergency Investigation - Fifth]	,
				[Accident And Emergency Investigation - Sixth]	,
				[Accident And Emergency Investigation - Seventh]	,
				[Accident And Emergency Investigation - Eighth]	,
				[Accident And Emergency Investigation - Ninth]	,
				[Accident And Emergency Investigation - Tenth]	,
				[Accident And Emergency Investigation - Eleventh]	,
				[Accident And Emergency Investigation - Twelfth]	,
				[Accident And Emergency Investigation - Thirteenth]	,
				[Accident And Emergency Investigation - Fourteenth]	,
				[Accident And Emergency Investigation - Fifteenth]	,
				[Accident And Emergency Investigation - Sixteenth]	,
				[Accident And Emergency Investigation - Seventeenth]	,
				[Accident And Emergency Investigation - Eighteenth]	,
				[Accident And Emergency Investigation - Nineteenth]	,
				[Accident And Emergency Investigation - Twentieth]	,
				[Accident And Emergency Investigation - Twenty First]	,
				[Accident And Emergency Investigation - Twenty Second]	,
				[Accident And Emergency Investigation - Twenty Third]	,
				[Accident And Emergency Investigation - Twenty Fourth]	
            )
        ) AS UNPVT
    WHERE INVESTIGATION_CODE<>''
)
SELECT 
PERSON_ID,
digest,
"START_DATE",
START_TIME,
cast(null as varchar) as END_DATE,
cast(null as varchar) as END_TIME,
INVESTIGATION_CODE,
INVESTIGATION_CODE_ORDER,
INVESTIGATION_CODE_SEQ,
'tbl_ec_backward_compatible_susplus' as SOURCE_TABLE
FROM   
source_data
;