{{ config(
    materialized='incremental'
    ,pre_hook="truncate table {{ this }}"
    ) }}
with source_data as(
    select 
person_id	as	person_id			,
ID	as	ID			,
[OSV Classification at CDS Activity Date]	as	OSV_Classification_at_CDS_Activity_Date			,
[Overseas Visitor Status Classification 1]	as	Overseas_Visitor_Status_Classification_1			,
[Overseas Visitor Status Start Date 1]	as	Overseas_Visitor_Status_Start_Date_1			,
[Overseas Visitor Status End Date 1]	as	Overseas_Visitor_Status_End_Date_1			,
[Overseas Visitor Status Classification 2]	as	Overseas_Visitor_Status_Classification_2			,
[Overseas Visitor Status Start Date 2]	as	Overseas_Visitor_Status_Start_Date_2			,
[Overseas Visitor Status End Date 2]	as	Overseas_Visitor_Status_End_Date_2			,
[Overseas Visitor Status Classification 3]	as	Overseas_Visitor_Status_Classification_3			,
[Overseas Visitor Status Start Date 3]	as	Overseas_Visitor_Status_Start_Date_3			,
[Overseas Visitor Status End Date 3]	as	Overseas_Visitor_Status_End_Date_3			,
[Overseas Visitor Status Classification 4]	as	Overseas_Visitor_Status_Classification_4			,
[Overseas Visitor Status Start Date 4]	as	Overseas_Visitor_Status_Start_Date_4			,
[Overseas Visitor Status End Date 4]	as	Overseas_Visitor_Status_End_Date_4			,
[Overseas Visitor Status Classification 5]	as	Overseas_Visitor_Status_Classification_5			,
[Overseas Visitor Status Start Date 5]	as	Overseas_Visitor_Status_Start_Date_5			,
[Overseas Visitor Status End Date 5]	as	Overseas_Visitor_Status_End_Date_5						
from  {{ source('warehouse', 'tbl_apc_finished_susplus') }}
where 
[Overseas Visitor Status Start Date 1] is not null
and   [Overseas Visitor Status Start Date 1]<>''
)
select * from source_data
