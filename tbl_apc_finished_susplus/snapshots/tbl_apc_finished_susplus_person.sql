{% snapshot tbl_apc_finished_susplus_person %}

--- Added a comment for testing

{{ config(
        target_schema= 'mart',
        unique_key= 'person_id',
        strategy= 'check',
        check_cols= ["postcode","Country" ]
      ) 
}}
select 
[person_id]
      ,[Reason_for_Access]
      ,[CDS_type]
      ,[birth_year]
      ,[birth_month]
      ,[Birth_Weight]
      ,[Live_or_Still_Birth]
      ,[Marital_Status_Psychiatric_Census_Only]
      ,[NHS_Number_Trace_Status]
      ,[Withheld_Identity_Reason]
      ,[Sex]
      ,[Organisation_Code_Residence_Responsibility]
      ,[Protocol_identifier]
      ,[Bulk_replacement_CDS_group]
      ,[postcode]
      ,[PCT_of_Residence]
      ,[Year_of_Birth_Mother]
      ,[Month_of_Birth_Mother]
      ,[Country]
      ,[1991_Census_ED]
      ,[ED_District_Code]
      ,[1998_Electoral_Area]
 from (
select  distinct 
[person_id]	AS	[person_id]	,
[Reason for Access]	AS	[Reason_for_Access]	,
[CDS type]	AS	[CDS_type]	,
[birth_year]	AS	[birth_year]	,
[birth_month]	AS	[birth_month]	,
[Birth Weight]	AS	[Birth_Weight]	,
[Live or Still Birth]	AS	[Live_or_Still_Birth]	,
[Marital Status (Psychiatric Census Only)]	AS	[Marital_Status_Psychiatric_Census_Only]	,
[NHS Number Trace Status]	AS	[NHS_Number_Trace_Status]	,
[Withheld Identity Reason]	AS	[Withheld_Identity_Reason]	,
[Sex]	AS	[Sex]	,
[Organisation Code (Residence Responsibility)]	AS	[Organisation_Code_Residence_Responsibility]	,
[Protocol identifier]	AS	[Protocol_identifier]	,
[Bulk replacement CDS group]	AS	[Bulk_replacement_CDS_group]	,
[postcode]	AS	[postcode]	,
[PCT of Residence]	AS	[PCT_of_Residence]	,
[Year of Birth Mother]	AS	[Year_of_Birth_Mother]	,
[Month of Birth Mother]	AS	[Month_of_Birth_Mother]	,
[Country]	AS	[Country]	,
[1991 Census ED]	AS	[1991_Census_ED]	,
[ED District Code]	AS	[ED_District_Code]	,
[1998 Electoral Area]	AS	[1998_Electoral_Area],
ROW_NUMBER() OVER (
    PARTITION BY [person_id] 
      ORDER BY try_cast( [Start Date (Consultant Episode)] as date) DESC ,[Start Time (Episode)] desc
    ) as cnt
 from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
 where [person_id]  is not null and [person_id]<>''
) src
  WHERE cnt=1

 {%  endsnapshot %}