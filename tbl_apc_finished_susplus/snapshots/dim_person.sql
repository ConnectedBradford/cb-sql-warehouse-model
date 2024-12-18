{% snapshot dim_person %}

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
      ,[birth_year]
      ,[birth_month]
      ,[Birth_Weight]
      ,[Live_or_Still_Birth]
      ,[Marital_Status_Psychiatric_Census_Only]
      ,[Withheld_Identity_Reason]
      ,[gender]
      ,[Organisation_Code_Residence_Responsibility]
      ,[postcode]
      ,[PCT_of_Residence]
      ,[Year_of_Birth_Mother]
      ,[Month_of_Birth_Mother]
      ,[Country]
      ,[1991_Census_ED]
      ,[ED_District_Code]
      ,[1998_Electoral_Area]
      ,[source_table]
      ,[source_table_date]
      ,[death_year]
      ,[death_month]
 from (
select  distinct 
[person_id]	AS	[person_id]	,
[birth_year]	AS	[birth_year]	,
[birth_month]	AS	[birth_month]	,
[Birth Weight]	AS	[Birth_Weight]	,
[Live or Still Birth]	AS	[Live_or_Still_Birth]	,
[Marital Status (Psychiatric Census Only)]	AS	[Marital_Status_Psychiatric_Census_Only]	,
[Withheld Identity Reason]	AS	[Withheld_Identity_Reason]	,
[Sex]	AS	[gender]	,
[Organisation Code (Residence Responsibility)]	AS	[Organisation_Code_Residence_Responsibility]	,
[postcode]	collate Latin1_General_CI_AS AS	[postcode]	,
[PCT of Residence]	AS	[PCT_of_Residence]	,
[Year of Birth Mother]	AS	[Year_of_Birth_Mother]	,
[Month of Birth Mother]	AS	[Month_of_Birth_Mother]	,
[Country]	collate Latin1_General_CI_AS AS	[Country]	,
[1991 Census ED]	AS	[1991_Census_ED]	,
[ED District Code]	AS	[ED_District_Code]	,
[1998 Electoral Area]	AS	[1998_Electoral_Area],
'tbl_apc_finished_susplus' as [source_table],
[Start Date (Hospital Provider Spell)] as source_table_date,
CAST(NULL AS VARCHAR) AS death_year,
CAST(NULL AS VARCHAR) AS death_month,
ROW_NUMBER() OVER (
    PARTITION BY [person_id] 
      ORDER BY try_cast( [Start Date (Consultant Episode)] as date) DESC ,[Start Time (Episode)] desc
    ) as cnt
 from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
 where [person_id]  is not null and [person_id]<>''
) src
  WHERE cnt=1

 {%  endsnapshot %}