{{ config(
    materialized='incremental'
    
    ) }}
--,pre_hook="truncate table {{ this }}"
with source_data as(
    select
person_id	as	person_id,	
[Arrival Date]	as	[Arrival_Date]	,
[Arrival Time]	as	[Arrival_Time]	,
[A&E Attendance Number]	as	[A&E_Attendance_Number]	,
[A&E Arrival Mode]	as	[A&E_Arrival_Mode]	,
[A&E Attendance Category]	as	[A&E_Attendance_Category]	,
[A&E Attendance Disposal]	as	[A&E_Attendance_Disposal]	,
[A&E Patient Group]	as	[A&E_Patient_Group]	,
[Source of Referral For A&E]	as	[Source_of_Referral_For_AE]	,
[A&E Departure Time]	as	[A&E_Departure_Time]	,
[A&E Departure Date]	as	[A&E_Departure_Date]	,
[A&E Initial Assessment Time (first and unplanned follow-up attendances only)]	as	[A&E_Initial_Assessment_Time]	,
[A&E Initial Assessment Date]	as	[A&E_Initial_Assessment_Date]	,
[A&E Time Seen For Treatment]	as	[A&E_Time_Seen_For_Treatment]	,
[Site Code (Of Treatment)]	as	[Site_Code_Of_Treatment]	,
[Ambulance Incident Number]	as	[Ambulance_Incident_Number]	,
[Area code derived from postcode ]	as	[Area_code_derived_from_postcode_]	,
[GP Practice (Derived)]	as	[GP_Practice_Derived]	,
[PCT (Derived from derived GP Practice)]	as	[PCT_Derived_from_derived_GP_Practice]	,
[SHA from GP Practice]	as	[SHA_from_GP_Practice]	
from
 {{ source('warehouse', 'tbl_ec_backward_compatible_susplus') }}
where person_id is not null
)
select * from source_data