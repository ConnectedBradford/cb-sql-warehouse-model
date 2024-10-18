{{ config(
    materialized='incremental'
    ,pre_hook="truncate table {{ this }}"
    ) }}

with source_data as (

SELECT 
person_id,START_DATE,START_TIME,END_DATE,END_TIME,[ICD_CODE],CODE_VALUE
    FROM
        (
        select 
        person_id,
        [Start Date (Hospital Provider Spell)] AS START_DATE,
        [Start Time (Hospital Provider Spell)] AS START_TIME,
        [Discharge Date (From Hospital Provider Spell)] AS END_DATE,
        [Discharge Time (Hospital Provider Spell)] AS END_TIME,
        --[Diagnosis Scheme In Use (ICD)],
        [Diagnosis Primary (ICD)],
        [Diagnosis 1st Secondary (ICD)],
        [Diagnosis 2nd Secondary (ICD)],
        [Diagnosis 3rd Secondary (ICD)],
        [Diagnosis 4th Secondary (ICD)],
        [Diagnosis 5th Secondary (ICD)],
        [Diagnosis 6th Secondary (ICD)],
        [Diagnosis 7th Secondary (ICD)],
        [Diagnosis 8th Secondary (ICD)],
        [Diagnosis 9th Secondary (ICD)],
        [Diagnosis 10th Secondary (ICD)],
        [Diagnosis 11th Secondary (ICD)],
        [Diagnosis 12th Secondary (ICD)],
        [Diagnosis 13th Secondary (ICD)],
        [Diagnosis 14th Secondary (ICD)],
        [Diagnosis 15th Secondary (ICD)],
        [Diagnosis 16th Secondary (ICD)],
        [Diagnosis 17th Secondary (ICD)],
        [Diagnosis 18th Secondary (ICD)],
        [Diagnosis 19th Secondary (ICD)],
        [Diagnosis 20th Secondary (ICD)],
        [Diagnosis 21st Secondary (ICD)],
        [Diagnosis 22nd Secondary (ICD)],
        [Diagnosis 23rd Secondary (ICD)]

        from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
        where 
        [Diagnosis Scheme In Use (ICD)] is not null---This value for valid records are 02
        ) As pv
        unpivot
        (
        CODE_VALUE FOR [ICD_CODE] IN 
            (
                [Diagnosis Primary (ICD)],
                [Diagnosis 1st Secondary (ICD)],
                [Diagnosis 2nd Secondary (ICD)],
                [Diagnosis 3rd Secondary (ICD)],
                [Diagnosis 4th Secondary (ICD)],
                [Diagnosis 5th Secondary (ICD)],
                [Diagnosis 6th Secondary (ICD)],
                [Diagnosis 7th Secondary (ICD)],
                [Diagnosis 8th Secondary (ICD)],
                [Diagnosis 9th Secondary (ICD)],
                [Diagnosis 10th Secondary (ICD)],
                [Diagnosis 11th Secondary (ICD)],
                [Diagnosis 12th Secondary (ICD)],
                [Diagnosis 13th Secondary (ICD)],
                [Diagnosis 14th Secondary (ICD)],
                [Diagnosis 15th Secondary (ICD)],
                [Diagnosis 16th Secondary (ICD)],
                [Diagnosis 17th Secondary (ICD)],
                [Diagnosis 18th Secondary (ICD)],
                [Diagnosis 19th Secondary (ICD)],
                [Diagnosis 20th Secondary (ICD)],
                [Diagnosis 21st Secondary (ICD)],
                [Diagnosis 22nd Secondary (ICD)],
                [Diagnosis 23rd Secondary (ICD)]
            )
        ) AS UNPVT
)
SELECT 
person_id,START_DATE,START_TIME,END_DATE,END_TIME,CODE_VALUE
FROM   
source_data WHERE CODE_VALUE<>'';