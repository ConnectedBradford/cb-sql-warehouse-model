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
		null as END_DATE,
		NULL AS END_TIME,
		CAST(V.ordinal AS VARCHAR) +'- Procedure' as OPCS_CODE_ORDER,
		V.ordinal OPCS_CODE_SEQ,
		v.value AS OPCS_CODE,
		'tbl_outpatient' AS SOURCE_TABLE,
		cast(null as varchar) as OPCS_DATE
		from
			(
			select 
			person_id,
			attendance_date start_date,
			attendance_time START_TIME,
			procedure_no 
			from 
			--dbo.tbl_outpatient  
			{{ source('warehouse', 'tbl_outpatient') }}
			where procedure_no is not null
			) a
		 CROSS APPLY STRING_SPLIT(a.procedure_no,'~',1) V  
		 )
	SELECT 
	PERSON_ID,
	START_DATE,
	START_TIME,
	END_DATE,
	END_TIME,
	OPCS_CODE_ORDER,
	OPCS_CODE,
	OPCS_DATE,
	OPCS_CODE_SEQ,
	source_table
	FROM SRC_DATA
 ;