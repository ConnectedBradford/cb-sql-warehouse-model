{{ config(
    materialized='incremental'
    ) }}

WITH SRC_DATA AS 
	(
	select 
		PERSON_ID,
		START_DATE,
		START_TIME,
		END_DATE,
		END_TIME,
		CAST(V.ordinal AS VARCHAR) +'- Diagnosis' as DIAG_CODE_ORDER,
		V.ordinal DIAG_CODE_SEQ,
		v.value AS DIAG_CODE,
		'tbl_episode' AS SOURCE_TABLE
		from
			(
			select 
			person_id,
			episode_start_date	as START_DATE,
			episode_start_time	AS START_TIME,
			episode_end_date	AS END_DATE,
			episode_end_time	AS END_TIME,
			diagnoses	
			from
				(
					select 
					person_id,
					episode_start_date,
					episode_start_time	,
					episode_end_date	,
					episode_end_time	,
					diagnoses	,
					row_number() over (partition by person_id,diagnoses  ORDER BY CONVERT( date,[episode_start_date_time] ) DESC ) as cnt
					from 
					--[src_StagingDatabase_Warehouse].dbo.tbl_episode
					{{ source('warehouse', 'tbl_episode') }}
					where diagnoses is not null
					
						) tbl_episode where cnt=1
			) a
		 CROSS APPLY STRING_SPLIT(a.diagnoses,'~',1) V  
		 )
	SELECT 
	PERSON_ID,
	CONVERT(varchar, START_DATE, 112) as  START_DATE,
	START_TIME,
	CONVERT(varchar, END_DATE, 112) as END_DATE,
	END_TIME,
	DIAG_CODE_ORDER,
	DIAG_CODE,
	DIAG_CODE_SEQ,
	source_table
	FROM SRC_DATA
 ;
