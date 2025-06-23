{{ config(
    materialized='incremental'
	,pre_hook="delete from {{ this }} where source_table= 'tbl_outpatient'" 
    ) }}

WITH SRC_DATA AS 
	(
	select 
		DISTINCT 
		PERSON_ID,
		digest,
		START_DATE,
		START_TIME,
		cast(null as date) as END_DATE,
		cast(null as time) AS END_TIME,
		CAST(V.ordinal AS VARCHAR) +'- Diagnosis' as DIAG_CODE_ORDER,
		V.ordinal DIAG_CODE_SEQ,
		v.value AS DIAG_CODE,
		'tbl_outpatient' AS SOURCE_TABLE
		from
			(
			select 
			person_id,
			digest,
			attendance_date start_date,
			attendance_time START_TIME,
			diagnosis_no 
			from 
			--dbo.tbl_outpatient 
			 {{ source('warehouse', 'tbl_outpatient') }} 
			where diagnosis_no is not null and digest is not null
			) a
		 CROSS APPLY STRING_SPLIT(a.diagnosis_no,'~',1) V  
		 )
	SELECT * FROM SRC_DATA
 ;
