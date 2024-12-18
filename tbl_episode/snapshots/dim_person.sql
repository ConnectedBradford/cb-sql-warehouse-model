{% snapshot dim_person %}

{{ config(
        target_schema= 'mart',
        unique_key= 'person_id',
        strategy= 'check',
        check_cols= ["birth_year"]
      ) 
}}

select

		  s.person_id	AS	[person_id]	,
          s.birth_year	AS	[birth_year]	,
          s.birth_month	AS	[birth_month]	,
          cast(NULL as varchar)	AS	[Birth_Weight]	,
          cast(NULL as varchar)	AS	[Live_or_Still_Birth]	,
          cast(NULL as varchar)	AS	[Marital_Status_Psychiatric_Census_Only]	,
          cast(NULL as varchar)	AS	[Withheld_Identity_Reason]	,
          s.gender	AS	[gender]	,
          cast(NULL as varchar)	AS	[Organisation_Code_Residence_Responsibility]	,
          cast(NULL as varchar)	AS	[postcode]	,
          cast(NULL as varchar)AS	[PCT_of_Residence]	,
          cast(NULL as varchar)	AS	[Year_of_Birth_Mother]	,
          cast(NULL as varchar)	AS	[Month_of_Birth_Mother]	,
          cast(NULL as varchar)	AS	[Country]	,
          cast(NULL as varchar)	AS	[1991_Census_ED]	,
          cast(NULL as varchar)AS	[ED_District_Code]	,
          cast(NULL as varchar)	AS	[1998_Electoral_Area],
          'tbl_episode' as [source_table],
          CONVERT(varchar, [episode_start_date_time], 112)  as [source_table_date],
          cast(s.death_year as varchar)	AS death_year,
          cast(s.death_month as varchar)	AS death_month

from 
	
	(select 
			person_id,
			gender,
			birth_year,
			birth_month,
			death_year,
			death_month,
			[episode_start_date_time]
		from 
			(
	
				select 
				 person_id,
				gender,
				birth_year,
				birth_month,
				death_year,
				death_month,
				[episode_start_date_time],
				ROW_NUMBER() OVER (PARTITION BY [person_id] ORDER BY CONVERT( date,[episode_start_date_time] ) DESC ) as cnt
				from 
         {{ source('warehouse', 'tbl_episode') }}
        
		
			) t where cnt=1
		)s
left join 
	(
		select person_id
		from 
    {{ source('mart_model', 'DIM_PERSON') }}
		where dbt_valid_to is null
	)d
on s.person_id = d.person_id 
where isnull(d.person_id,999) =999

 {%  endsnapshot %}