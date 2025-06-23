{{ config(
    materialized='incremental'
	,pre_hook="delete from {{ this }} where source_table= 'tbl_ae'" 
    ) }}

with source_data as (

SELECT 
person_id,
digest,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
DIAG_CODE_ORDER,
DIAG_CODE,
ROW_NUMBER() OVER
 ( PARTITION BY PERSON_ID,digest,"START_DATE",START_TIME,END_DATE,
		END_TIME
		ORDER BY 
		(CASE 
			WHEN DIAG_CODE_ORDER='diagnosis_1' THEN 1 
			WHEN DIAG_CODE_ORDER='diagnosis_2' THEN 2
			WHEN DIAG_CODE_ORDER='diagnosis_3' THEN 3
			WHEN DIAG_CODE_ORDER='diagnosis_4' THEN 4
			WHEN DIAG_CODE_ORDER='diagnosis_5' THEN 5
			WHEN DIAG_CODE_ORDER='diagnosis_6' THEN 6
			WHEN DIAG_CODE_ORDER='diagnosis_7' THEN 7
			WHEN DIAG_CODE_ORDER='diagnosis_8' THEN 8
			WHEN DIAG_CODE_ORDER='diagnosis_9' THEN 9
			WHEN DIAG_CODE_ORDER='diagnosis_10' THEN 10
			WHEN DIAG_CODE_ORDER='diagnosis_11' THEN 11
			WHEN DIAG_CODE_ORDER='diagnosis_12' THEN 12
			END ) 
		) AS DIAG_CODE_SEQ

    FROM
        (
        select
		person_id,
		digest,
		case when isnumeric(diagnosis_1)=0 then diagnosis_1 else null end diagnosis_1,
		case when isnumeric(diagnosis_2)=0 then diagnosis_2 else null end diagnosis_2,
		case when isnumeric(diagnosis_3)=0 then diagnosis_3 else null end diagnosis_3,
		case when isnumeric(diagnosis_4)=0 then diagnosis_4 else null end diagnosis_4,
		case when isnumeric(diagnosis_5)=0 then diagnosis_5 else null end diagnosis_5,
		case when isnumeric(diagnosis_6)=0 then diagnosis_6 else null end diagnosis_6,
		case when isnumeric(diagnosis_7)=0 then diagnosis_7 else null end diagnosis_7,
		case when isnumeric(diagnosis_8)=0 then diagnosis_8 else null end diagnosis_8,
		case when isnumeric(diagnosis_9)=0 then diagnosis_9 else null end diagnosis_9,
		case when isnumeric(diagnosis_10)=0 then diagnosis_10 else null end diagnosis_10,
		case when isnumeric(diagnosis_11)=0 then diagnosis_11 else null end diagnosis_11,
		case when isnumeric(diagnosis_12)=0 then diagnosis_12 else null end diagnosis_12,
		start_date,
		start_time,
		end_date,
		end_time
		from 
					(
	
				select  
						person_id,
						digest,
						replace(TRANSLATE(diagnosis_1, '/.', '..'),'.','') as diagnosis_1,
						replace(TRANSLATE(diagnosis_2, '/.', '..'),'.','') as diagnosis_2,
						replace(TRANSLATE(diagnosis_3, '/.', '..'),'.','') as diagnosis_3,
						replace(TRANSLATE(diagnosis_4, '/.', '..'),'.','') as diagnosis_4,
						replace(TRANSLATE(diagnosis_5, '/.', '..'),'.','') as diagnosis_5,
						replace(TRANSLATE(diagnosis_6, '/.', '..'),'.','') as diagnosis_6,
						replace(TRANSLATE(diagnosis_7, '/.', '..'),'.','') as diagnosis_7,
						replace(TRANSLATE(diagnosis_8, '/.', '..'),'.','') as diagnosis_8,
						replace(TRANSLATE(diagnosis_9, '/.', '..'),'.','') as diagnosis_9,
						replace(TRANSLATE(diagnosis_10, '/.', '..'),'.','') as diagnosis_10,
						replace(TRANSLATE(diagnosis_11, '/.', '..'),'.','') as diagnosis_11,
						replace(TRANSLATE(diagnosis_12, '/.', '..'),'.','') as diagnosis_12,
						CONVERT(varchar,arrival_date_time,112) start_date,
						CONVERT(VARCHAR, arrival_date_time, 108) start_time,
						CONVERT(varchar,departure_date_time,112) end_date,
						CONVERT(VARCHAR, departure_date_time, 108) end_time
						from 
						--tbl_ae a commented
						{{ source('warehouse', 'tbl_ae') }}
						where 
						 digest is not null
						  and (
						diagnosis_1 is not null or
						diagnosis_2 is not null or
						diagnosis_3 is not null or
						diagnosis_4 is not null or
						diagnosis_5 is not null or
						diagnosis_6 is not null or
						diagnosis_7 is not null or
						diagnosis_8 is not null or
						diagnosis_9 is not null  or
						diagnosis_10 is not null or
						diagnosis_11 is not null or
						diagnosis_12 is not null 
						)
			--person_id=272632
				) trans
			) As pv
        unpivot
        (
        DIAG_CODE FOR DIAG_CODE_ORDER IN 
            (
                diagnosis_1,
				diagnosis_2,
				diagnosis_3,
				diagnosis_4,
				diagnosis_5,
				diagnosis_6,
				diagnosis_7,
				diagnosis_8,
				diagnosis_9,
				diagnosis_10,
				diagnosis_11,
				diagnosis_12
            )
        ) AS UNPVT
    WHERE DIAG_CODE<>'' and DIAG_CODE<>'XXXX'
)
SELECT 
PERSON_ID,
digest,
"START_DATE",
START_TIME,
END_DATE,
END_TIME,
DIAG_CODE_ORDER,
DIAG_CODE,
DIAG_CODE_SEQ,
'tbl_ae' as source_table
FROM   
source_data 
