{{ config(
    materialized='incremental'
	,pre_hook="truncate table {{ this }}"
    ) }}

with source_data as (
    	select 
	PERSON_ID,
	START_DATE,
	START_TIME,
	END_DATE,
	END_TIME,
	--OPCS_CODE_Des,--[Primary Procedure (OPCS)] ,[2nd Procedure (OPCS)] etc..
	OPCS_CODE,
	OPCS_DATE
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
			OPCS_CODE FOR OPCS_CODE_Des IN
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


WHERE LEFT(OPCS_CODE_Des,2) = LEFT(OPCS_DATE_Des,2)   AND OPCS_CODE<>''
)
SELECT * FROM source_data
;
