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
		CAST(V.ordinal AS VARCHAR) +'- Investigation'INVESTIGATION_CODE_ORDER,
		V.ordinal INVESTIGATION_CODE_SEQ,
		v.value AS INVESTIGATION_CODE,
		'tbl_outpatient' AS SOURCE_TABLE
		from
			(
			select 
			person_id,
			attendance_date start_date,
			attendance_time START_TIME,
			investigation_no 
			from 
			--dbo.tbl_outpatient  
			 {{ source('warehouse', 'tbl_outpatient') }}
			where investigation_no is not null
			) a
		 CROSS APPLY STRING_SPLIT(a.investigation_no,'~',1) V  
		 )
	SELECT * FROM SRC_DATA
 ;