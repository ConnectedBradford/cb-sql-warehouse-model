{{ config(
    materialized='incremental'
    ,pre_hook="truncate table {{ this }}"
    ) }}
with source_data as(
    select 
person_id	as	person_id,
ID	as	ID,
[Consultant Code]	as	Consultant_Code,
[Start Date (Consultant Episode)]	as	Start_Date,
[Start Time (Episode)]	as	Start_Time,
[End Date (Consultant Episode)]	as	End_Date,
[End Time (Episode)]	as	End_Time,
[Administrative Category]	as	Administrative_Category,
[Patient Classification]	as	Patient_Classification,
[Admission Method (Hospital Provider Spell)]	as	Admission_Method,
[Discharge Destination (Hospital Provider Spell)]	as	Discharge_Destination,
[Discharge Method (Hospital Provider Spell)]	as	Discharge_Method,
[Source of Admission (Hospital Provider Spell)]	as	Source_of_Admission,
[Start Date (Hospital Provider Spell)]	as	Start_Date_Hospital_Provider_Spell,
[Start Time (Hospital Provider Spell)]	as	Start_Time_Hospital_Provider_Spell,
[Discharge Date (From Hospital Provider Spell)]	as	Discharge_Date,
[Discharge Time (Hospital Provider Spell)]	as	Discharge_Time,
[Discharge To Hospital At Home Service Indicator]	as	Discharge_To_Hospital_Indicator,
[Episode Number]	as	Episode_Number,
[First Regular Day Night Admission]	as	First_Regular_Day_Night_Admission,
[Last Episode In Spell Indicator]	as	Last_Episode_In_Spell_Indicator,
[Neonatal Level of Care]	as	Neonatal_Level_of_Care,
[Operation Status]	as	Operation_Status,
[Psychiatric Patient Status]	as	Psychiatric_Patient_Status,
[Length Of Stay Adjustment (Rehabilitation)]	as	Length_Of_Stay_Adjustment_Rehabilitation,
[Length Of Stay Adjustment (Specialist Palliative Care)]	as	Length_Of_Stay_Adjustment_Specialist_Palliative_Care,
[Commissioning Serial Number]	as	Commissioning_Serial_Number,
[NHS Service Agreement Line Number]	as	NHS_Service_Agreement_Line_Number,
[Commissioner Reference Number]	as	Commissioner_Reference_Number,
[Organisation Code Code of Provider]	as	Organisation_Code_Code_of_Provider,
[Organisation Code Code of Commissioner]	as	Organisation_Code_Code_of_Commissioner,
[Main Specialty Code]	as	Main_Specialty_Code,
[Treatment Function Code]	as	Treatment_Function_Code,
[Local Sub Specialty Code]	as	Local_Sub_Specialty_Code,
[Rehabilitation Assessment Team Type]	as	Rehabilitation_Assessment_Team_Type
from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
)
select * from source_data
;