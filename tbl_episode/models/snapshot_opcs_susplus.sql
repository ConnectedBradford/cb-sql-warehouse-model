{{ config(
    materialized='incremental'
	
    ) }}

WITH SRC_DATA AS 
	(
	select 
		DISTINCT 
		PERSON_ID,
		START_DATE,
		START_TIME,
		END_DATE,
		END_TIME,
		CAST(V.ordinal AS VARCHAR) +'- Procedure' as OPCS_CODE_ORDER,
		V.ordinal OPCS_CODE_SEQ,
		v.value AS OPCS_CODE,
		'tbl_episode' AS SOURCE_TABLE
		from
			(
			select 
			person_id,
			episode_start_date	as START_DATE,
			episode_start_time	AS START_TIME,
			episode_end_date	AS END_DATE,
			episode_end_time	AS END_TIME,
			procedures	
			from
				(
					select 
					person_id,
					episode_start_date,
					episode_start_time	,
					episode_end_date	,
					episode_end_time	,
					procedures	,
					row_number() over (partition by person_id,diagnoses  ORDER BY CONVERT( date,[episode_start_date_time] ) DESC ) as cnt
					from 
					--[src_StagingDatabase_Warehouse].dbo.tbl_episode
					{{ source('warehouse', 'tbl_episode') }}
					where procedures is not null
					
						) tbl_episode where cnt=1
			) a
		 CROSS APPLY STRING_SPLIT(a.procedures,'~',1) V  
		 )
	SELECT 
	PERSON_ID,
	CONVERT(varchar, START_DATE, 112) as  START_DATE,
	START_TIME,
	CONVERT(varchar, END_DATE, 112) as END_DATE,
	END_TIME,
	OPCS_CODE_ORDER,
	OPCS_CODE,
	CONVERT(varchar, START_DATE, 112) as OPCS_DATE,
	OPCS_CODE_SEQ,
	source_table
	FROM SRC_DATA
 ;