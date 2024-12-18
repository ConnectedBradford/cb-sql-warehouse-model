{% snapshot dim_person %}

{{ config(
        target_schema= 'mart',
        unique_key= 'person_id',
        strategy= 'check',
        check_cols= ["postcode","death_year","death_month"]
      ) 
}}

select 
      [person_id],
      [birth_year],
      [birth_month],
      cast(NULL as varchar)	AS  [Birth_Weight],
      cast(NULL as varchar)	AS	[Live_or_Still_Birth]	,
      cast(NULL as varchar)	AS	[Marital_Status_Psychiatric_Census_Only]	,
      cast(NULL as varchar)	AS	[Withheld_Identity_Reason]	,
      [gender],
      cast(NULL as varchar)	AS	[Organisation_Code_Residence_Responsibility]	,
	  [postcode],
      cast(NULL as varchar)AS	[PCT_of_Residence]	,
      cast(NULL as varchar)	AS	[Year_of_Birth_Mother]	,
      cast(NULL as varchar)	AS	[Month_of_Birth_Mother]	,
      cast(NULL as varchar)	AS	[Country]	,
      cast(NULL as varchar)	AS	[1991_Census_ED]	,
      cast(NULL as varchar)AS	[ED_District_Code]	,
      cast(NULL as varchar)	AS	[1998_Electoral_Area],
	  'tbl_ae' as [source_table],
      source_table_date,
      [death_year],
      [death_month]
				 from (

				select 
				person_id,
				birth_year,
				birth_month,
				gender,
				cast(death_year as varchar) collate Latin1_General_CI_AS as death_year,
				cast(death_month as varchar) collate Latin1_General_CI_AS as death_month ,
				postcode collate Latin1_General_CI_AS as postcode,
				convert(varchar,arrival_date_time,112) as source_table_date
				from 
				(
							select 
							person_id,
							birth_year,
							birth_month,
							gender,
							death_year,
							death_month,
							postcode,
							arrival_date_time,
							row_number() over (partition by person_id order by isnull(gender,99),arrival_date_time desc) as cnt from 
							(

									select 
									person_id,
									birth_year,
									birth_month,
									gender,
									death_year,
									death_month,
									postcode,
									arrival_date_time
									from 
									--tbl_ae a
									{{ source('warehouse', 'tbl_ae') }}
									where  person_id is not null 
									union all
									select 
									person_id,
									birth_year,
									birth_month,
									null as gender,
									death_year,
									death_month,
									postcode,
									INJURY_DT_TM as arrival_date_time
									from 
									--tbl_ae_nautilus a
									{{ source('warehouse', 'tbl_ae_nautilus') }}
									where person_id is not null 
								) a
					) whole where cnt=1
	) main

 {%  endsnapshot %}