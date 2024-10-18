{{ config(
    materialized='incremental'
    ,pre_hook="truncate table {{ this }}"
    ) }}
with source_data as(
    select

        person_id AS person_id,
        id as id,
        [Extract date time]	as	Extract_date_time	,
        [Report period Start Date]	as	Report_period_Start_Date	,
        [Report period End Date]	as	Report_period_End_Date	,
        [Organisation code  Sender of transaction]	as	Organisation_code__Sender_of_transaction	,
        [Submission Date]	as	Submission_Date	,
        [Organisation Code (Local Patient Identifier)]	as	Organisation_Code_Local_Patient_Identifier	,
        [General Medical Practitioner Code of Registered GMP]	as	General_Medical_Practitioner_Code_of_Registered_GMP	,
        [Practice Code of Registered GP]	as	Practice_Code_of_Registered_GP	,
        [Referrer Code]	as	Referrer_Code	,
        [Referring Organisation Code]	as	Referring_Organisation_Code	,
        [Duration of Elective Wait]	as	Duration_of_Elective_Wait	,
        [Intended Management]	as	Intended_Management	,
        [Decided To Admit Date (For This Provider)]	as	Decided_To_Admit_Date_For_This_Provider	,
        [Waiting Time Measurement Type]	as	Waiting_Time_Measurement_Type	,
        [FCE HRG]	as	FCE_HRG	,
        [Episode HRG Version Number]	as	Episode_HRG_Version_Number	,
        [Spell Core HRG]	as	Spell_Core_HRG	,
        [Spell HRG Version Number]	as	Spell_HRG_Version_Number	,
        [Patient Pathway Identifier]	as	Patient_Pathway_Identifier	,
        [Organisation Code Patient Pathway Identifier Issuer]	as	Organisation_Code_Patient_Pathway_Identifier_Issuer	,
        [Referral To Treatment Period Status]	as	Referral_To_Treatment_Period_Status	,
        [Referral To Treatment Period Start Date]	as	Referral_To_Treatment_Period_Start_Date	,
        [Referral To Treatment Period End Date]	as	Referral_To_Treatment_Period_End_Date	,
        [CDS Activity Date]	as	CDS_Activity_Date	,
        [Hospital Provider Spell Discharge Ready Date]	as	Hospital_Provider_Spell_Discharge_Ready_Date	,
        [XML Version]	as	XML_Version	,
        [Referral To Treatment Length (Derived)]	as	Referral_To_Treatment_Length_Derived	,
        [Age range patient derived from DOB]	as	Age_range_patient_derived_from_DOB	,
        [Age range mother derived from DOB]	as	Age_range_mother_derived_from_DOB	,
        [Area code derived from postcode ]	as	Area_code_derived_from_postcode_	,
        [CDS Group]	as	CDS_Group	,
        [Finished Indicator]	as	Finished_Indicator	,
        [GP Practice (Derived)]	as	GP_Practice_Derived	,
        [GP Practice Mother (Derived)]	as	GP_Practice_Mother_Derived	,
        [PCT (Derived from derived GP Practice)]	as	PCT_Derived_from_derived_GP_Practice	,
        [PCT Mother (Derived from derived GP Practice)]	as	PCT_Mother_Derived_from_derived_GP_Practice	,
        [Hospital Spell Duration]	as	Hospital_Spell_Duration	,
        [Month of Birth]	as	Month_of_Birth	,
        [Home Birth or Delivery]	as	Home_Birth_or_Delivery	,
        [PCT from postcode]	as	PCT_from_postcode	,
        [PCT Type from Postcode]	as	PCT_Type_from_Postcode	

from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
)
select * from source_data
