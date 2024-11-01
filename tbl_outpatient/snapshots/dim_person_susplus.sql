{% snapshot dim_person_susplus %}

{{ config(
        target_schema= 'mart',
        unique_key= 'person_id',
        strategy= 'check',
        check_cols= ["postcode"]
      ) 
}}

SELECT 
S.*
FROM 
(
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
          cast(NULL as varchar)	AS	[Birth_Weight]	,
          cast(NULL as varchar)	AS	[Live_or_Still_Birth]	,
          cast(NULL as varchar)	AS	[Marital_Status_Psychiatric_Census_Only]	,
          cast(NULL as varchar)	AS	[Withheld_Identity_Reason]	,
          [gender]	AS	[gender]	,
          cast(NULL as varchar)	AS	[Organisation_Code_Residence_Responsibility]	,
          [postcode]	AS	[postcode]	,
          cast(NULL as varchar)AS	[PCT_of_Residence]	,
          cast(NULL as varchar)	AS	[Year_of_Birth_Mother]	,
          cast(NULL as varchar)	AS	[Month_of_Birth_Mother]	,
          cast(NULL as varchar)	AS	[Country]	,
          cast(NULL as varchar)	AS	[1991_Census_ED]	,
          cast(NULL as varchar)AS	[ED_District_Code]	,
          cast(NULL as varchar)	AS	[1998_Electoral_Area],
          'tbl_outpatient' as [source_table],
          CONVERT(varchar, [attendance_date], 112)  as [source_table_date],
          death_year	AS death_year,
          death_month	AS death_month,
          ROW_NUMBER() OVER (PARTITION BY [person_id] ORDER BY CONVERT( date,[attendance_date] ) DESC ) as cnt
          from 
          --dbo.tbl_outpatient
          {{ source('warehouse', 'tbl_outpatient') }}
            where [person_id]  is not null and [person_id]<>''
          ) src
         WHERE cnt=1
	)	S
LEFT JOIN 
(
  SELECT PERSON_ID,postcode,SOURCE_TABLE_DATE 
  FROM 
  {{ source('mart_model', 'DIM_PERSON_SUSPLUS') }}
  --[CB_MYSPACE_JA_1].MART.DIM_PERSON_SUSPLUS
  WHERE dbt_valid_to IS NULL
) D
ON S.person_id =D.person_id 
WHERE S.source_table_date > D.source_table_date
AND ISNULL(S.postcode,'X') collate Latin1_General_CS_AS <>ISNULL(D.POSTCODE,'X')  collate Latin1_General_CS_AS 

 {%  endsnapshot %}