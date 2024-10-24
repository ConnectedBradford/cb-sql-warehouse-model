{{ config(
    materialized='incremental'
	
    ) }}

with source_data as (
    	select 
	PERSON_ID,
	START_DATE,
	START_TIME,
	END_DATE,
	END_TIME,
	OPCS_CODE_ORDER,--[Primary Procedure (OPCS)] ,[2nd Procedure (OPCS)] etc..
	OPCS_CODE,
	OPCS_DATE,
	ROW_NUMBER() OVER 
	(
		PARTITION BY PERSON_ID,START_DATE,START_TIME,END_DATE,
		END_TIME
		ORDER BY 
		 ( CASE 
		 WHEN OPCS_CODE_ORDER='Primary Procedure (OPCS)' THEN 1
		 WHEN OPCS_CODE_ORDER='2nd Procedure (OPCS)' THEN 2
		 WHEN OPCS_CODE_ORDER='3rd Procedure (OPCS)' THEN 3
		 WHEN OPCS_CODE_ORDER='4th Procedure (OPCS)' THEN 4
		 WHEN OPCS_CODE_ORDER='5th Procedure (OPCS)' THEN 5
		 WHEN OPCS_CODE_ORDER='6th Procedure (OPCS)' THEN 6
		 WHEN OPCS_CODE_ORDER='7th Procedure (OPCS)' THEN 7
		 WHEN OPCS_CODE_ORDER='8th Procedure (OPCS)' THEN 8
		 WHEN OPCS_CODE_ORDER='9th Procedure (OPCS)' THEN 9
		 WHEN OPCS_CODE_ORDER='10th Procedure (OPCS)' THEN 10
		 WHEN OPCS_CODE_ORDER='11th Procedure (OPCS)' THEN 11
		 WHEN OPCS_CODE_ORDER='12th Procedure (OPCS)' THEN 12
		 WHEN OPCS_CODE_ORDER='13th Procedure (OPCS)' THEN 13
		 WHEN OPCS_CODE_ORDER='14th Procedure (OPCS)' THEN 14
		 WHEN OPCS_CODE_ORDER='15th Procedure (OPCS)' THEN 15
		 WHEN OPCS_CODE_ORDER='16th Procedure (OPCS)' THEN 16
		 WHEN OPCS_CODE_ORDER='17th Procedure (OPCS)' THEN 17
		 WHEN OPCS_CODE_ORDER='18th Procedure (OPCS)' THEN 18
		 WHEN OPCS_CODE_ORDER='19th Procedure (OPCS)' THEN 19
		 WHEN OPCS_CODE_ORDER='20th Procedure (OPCS)' THEN 20
		 WHEN OPCS_CODE_ORDER='21st Procedure (OPCS)' THEN 21
		 WHEN OPCS_CODE_ORDER='22nd Procedure (OPCS)' THEN 22
		 WHEN OPCS_CODE_ORDER='23rd Procedure (OPCS)' THEN 23
		 WHEN OPCS_CODE_ORDER='24th Procedure (OPCS)' THEN 24
		 END )
	) AS OPCS_CODE_SEQ
	--OPCS_DATE_Des --[Primary Procedure Date (OPCS)],[2nd Procedure Date (OPCS)] etc...
	--RIGHT(OPCS_CODE,1) ,
	--RIGHT(OPCS_DATE,1)

from
		(
		select 
			person_id	,
			[Start Date (Hospital Provider Spell)] AS START_DATE,
			[Start Time (Hospital Provider Spell)] AS START_TIME,
			[Discharge Date (From Hospital Provider Spell)] AS END_DATE,
			[Discharge Time (Hospital Provider Spell)] AS END_TIME	,
			--[Procedure Scheme In Use (OPCS)]	,
			[Primary Procedure (OPCS)]	,
			[Primary Procedure Date (OPCS)]	,
			[2nd Procedure (OPCS)]	,
			[2nd Procedure Date (OPCS)]	,
			[3rd Procedure (OPCS)]	,
			[3rd Procedure Date (OPCS)]	,
			[4th Procedure (OPCS)]	,
			[4th Procedure Date (OPCS)]	,
			[5th Procedure (OPCS)]	,
			[5th Procedure Date (OPCS)]	,
			[6th Procedure (OPCS)]	,
			[6th Procedure Date (OPCS)]	,
			[7th Procedure (OPCS)]	,
			[7th Procedure Date (OPCS)]	,
			[8th Procedure (OPCS)]	,
			[8th Procedure Date (OPCS)]	,
			[9th Procedure (OPCS)]	,
			[9th Procedure Date (OPCS)]	,
			[10th Procedure (OPCS)]	,
			[10th Procedure Date (OPCS)]	,
			[11th Procedure (OPCS)]	,
			[11th Procedure Date (OPCS)]	,
			[12th Procedure (OPCS)]	,
			[12th Procedure Date (OPCS)]	,
			[13th Procedure (OPCS)]	,
			[13th Procedure Date (OPCS)]	,
			[14th Procedure (OPCS)]	,
			[14th Procedure Date (OPCS)]	,
			[15th Procedure (OPCS)]	,
			[15th Procedure Date (OPCS)]	,
			[16th Procedure (OPCS)]	,
			[16th Procedure Date (OPCS)]	,
			[17th Procedure (OPCS)]	,
			[17th Procedure Date (OPCS)]	,
			[18th Procedure (OPCS)]	,
			[18th Procedure Date (OPCS)]	,
			[19th Procedure (OPCS)]	,
			[19th Procedure Date (OPCS)]	,
			[20th Procedure (OPCS)]	,
			[20th Procedure Date (OPCS)]	,
			[21st Procedure (OPCS)]	,
			[21st Procedure Date (OPCS)]	,
			[22nd Procedure (OPCS)]	,
			[22nd Procedure Date (OPCS)]	,
			[23rd Procedure (OPCS)]	,
			[23rd Procedure Date (OPCS)]	,
			[24th Procedure (OPCS)]	,
			[24th Procedure Date (OPCS)]	
		from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
		where 
            [Procedure Scheme In Use (OPCS)] is not null
            and [Procedure Scheme In Use (OPCS)]<>'' 
			 
		) as pv
		unpivot
		( 
			OPCS_CODE FOR OPCS_CODE_ORDER IN
				(
				[Primary Procedure (OPCS)]	,
				[2nd Procedure (OPCS)]	,
				[3rd Procedure (OPCS)]	,
				[4th Procedure (OPCS)]	,
				[5th Procedure (OPCS)]	,
				[6th Procedure (OPCS)]	,
				[7th Procedure (OPCS)]	,
				[8th Procedure (OPCS)]	,
				[9th Procedure (OPCS)]	,
				[10th Procedure (OPCS)]	,
				[11th Procedure (OPCS)]	,
				[12th Procedure (OPCS)]	,
				[13th Procedure (OPCS)]	,
				[14th Procedure (OPCS)]	,
				[15th Procedure (OPCS)]	,
				[16th Procedure (OPCS)]	,
				[17th Procedure (OPCS)]	,
				[18th Procedure (OPCS)]	,
				[19th Procedure (OPCS)]	,
				[20th Procedure (OPCS)]	,
				[21st Procedure (OPCS)]	,
				[22nd Procedure (OPCS)]	,
				[23rd Procedure (OPCS)]	,
				[24th Procedure (OPCS)]	
			
				)
		) UNPVT
		UNPIVOT
		(
			OPCS_DATE For OPCS_DATE_Des IN 
			(	[Primary Procedure Date (OPCS)], 
				[2nd Procedure Date (OPCS)] ,
				[3rd Procedure Date (OPCS)]	,
				[4th Procedure Date (OPCS)]	,
				[5th Procedure Date (OPCS)]	,
				[6th Procedure Date (OPCS)]	,
				[7th Procedure Date (OPCS)]	,
				[8th Procedure Date (OPCS)]	,
				[9th Procedure Date (OPCS)]	,
				[10th Procedure Date (OPCS)]	,
				[11th Procedure Date (OPCS)]	,
				[12th Procedure Date (OPCS)]	,
				[13th Procedure Date (OPCS)]	,
				[14th Procedure Date (OPCS)]	,
				[15th Procedure Date (OPCS)]	,
				[16th Procedure Date (OPCS)]	,
				[17th Procedure Date (OPCS)]	,
				[18th Procedure Date (OPCS)]	,
				[19th Procedure Date (OPCS)]	,
				[20th Procedure Date (OPCS)]	,
				[21st Procedure Date (OPCS)]	,
				[22nd Procedure Date (OPCS)]	,
				[23rd Procedure Date (OPCS)]	,
				[24th Procedure Date (OPCS)]	
			)
		) pvt2	

WHERE LEFT(OPCS_CODE_ORDER,2) = LEFT(OPCS_DATE_Des,2)   AND OPCS_CODE<>''
)
SELECT 
PERSON_ID,
START_DATE,
START_TIME,
END_DATE,
END_TIME,
OPCS_CODE_ORDER,
OPCS_CODE,
OPCS_DATE,OPCS_CODE_SEQ
FROM source_data
--ORDER BY 
--1,2,3,4,5,9
--OPCS_CODE_SEQ_1
;

