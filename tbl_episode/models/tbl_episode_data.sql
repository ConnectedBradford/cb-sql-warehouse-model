{{ config(
    materialized='incremental'
    
    ) }}
--,pre_hook="truncate table {{ this }}"
with source_data as(
   select 
person_id,
practice	,
episode_start_date_time	,
episode_end_date_time	,
episode_start_date	,
episode_start_time	,
episode_end_date	,
episode_end_time	,
episode_duration	,
current_episode	,
last_episode_in_spell	,
operation_status	,
start_ward	,
end_ward	,
start_treatment_function_local	,
start_treatment_function	,
end_treatment_function_local	,
end_treatment_function	,
consultant_specialty_local	,
consultant_specialty	,
start_site_local	,
start_site	,
end_site_local	,
end_site	,
provider	,
purchaser	,
consultant_local	,
consultant	,
neonatal_level_of_care_local	,
neonatal_level_of_care	,
pathway_id	,
booking_reference_number	,
pathway_id_issuer	,
rtt_period_status	,
commissioning_serial_number	,
hrg_code_episode	,
hrg_code_spell	,
pss_fce_npoc	,
pss_service_line	,
pss_spell_service_line	,
pss_commissioner_code	
from
-- [src_StagingDatabase_Warehouse].dbo.tbl_episode
{{ source('warehouse', 'tbl_episode') }}
)
select * from source_data
;