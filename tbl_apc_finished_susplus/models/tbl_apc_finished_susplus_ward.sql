{{ config(
    materialized='incremental'
    ,pre_hook="truncate table {{ this }}"
    ) }}
with source_data as(
    select 
person_id	as	person_id	,
ID	as	ID	,
[Ward Code at Episode Start Date]	as	Ward_Code_at_Episode_Start_Date	,
[Location Class at Episode Start Date]	as	Location_Class_at_Episode_Start_Date	,
[Site Code (of Treatment) At Episode Start Date]	as	Site_Code_of_Treatment_At_Episode_Start_Date	,
[Ward Code 1]	as	Ward_Code_1	,
[Location Class 1]	as	Location_Class_1	,
[Site Code (of Treatment) 1]	as	Site_Code_of_Treatment_1	,
[Start Date 1]	as	Start_Date_1	,
[Start Time (Ward Stay) 1]	as	Start_Time_Ward_Stay_1	,
[End Date 1]	as	End_Date_1	,
[End Time (Ward Stay) 1]	as	End_Time_Ward_Stay_1	,
[Ward Code 2]	as	Ward_Code_2	,
[Location Class 2]	as	Location_Class_2	,
[Site Code (of Treatment) 2]	as	Site_Code_of_Treatment_2	,
[Start Date 2]	as	Start_Date_2	,
[Start Time (Ward Stay) 2]	as	Start_Time_Ward_Stay_2	,
[End Date 2]	as	End_Date_2	,
[End Time (Ward Stay) 2]	as	End_Time_Ward_Stay_2	,
[Ward Code 3]	as	Ward_Code_3	,
[Location Class 3]	as	Location_Class_3	,
[Site Code (of Treatment) 3]	as	Site_Code_of_Treatment_3	,
[Start Date 3]	as	Start_Date_3	,
[Start Time (Ward Stay) 3]	as	Start_Time_Ward_Stay_3	,
[End Date 3]	as	End_Date_3	,
[End Time (Ward Stay) 3]	as	End_Time_Ward_Stay_3	,
[Ward Code 4]	as	Ward_Code_4	,
[Location Class 4]	as	Location_Class_4	,
[Site Code (of Treatment) 4]	as	Site_Code_of_Treatment_4	,
[Start Date 4]	as	Start_Date_4	,
[Start Time (Ward Stay) 4]	as	Start_Time_Ward_Stay_4	,
[End Date 4]	as	End_Date_4	,
[End Time (Ward Stay) 4]	as	End_Time_Ward_Stay_4	,
[Ward Code 5]	as	Ward_Code_5	,
[Location Class 5]	as	Location_Class_5	,
[Site Code (of Treatment) 5]	as	Site_Code_of_Treatment_5	,
[Start Date 5]	as	Start_Date_5	,
[Start Time (Ward Stay) 5]	as	Start_Time_Ward_Stay_5	,
[End Date 5]	as	End_Date_5	,
[End Time (Ward Stay) 5]	as	End_Time_Ward_Stay_5	,
[Ward Code 6]	as	Ward_Code_6	,
[Location Class 6]	as	Location_Class_6	,
[Site Code (of Treatment) 6]	as	Site_Code_of_Treatment_6	,
[Start Date 6]	as	Start_Date_6	,
[Start Time (Ward Stay) 6]	as	Start_Time_Ward_Stay_6	,
[End Date 6]	as	End_Date_6	,
[End Time (Ward Stay) 6]	as	End_Time_Ward_Stay_6	,
[Ward Code 7]	as	Ward_Code_7	,
[Location Class 7]	as	Location_Class_7	,
[Site Code (of Treatment) 7]	as	Site_Code_of_Treatment_7	,
[Start Date 7]	as	Start_Date_7	,
[Start Time (Ward Stay) 7]	as	Start_Time_Ward_Stay_7	,
[End Date 7]	as	End_Date_7	,
[End Time (Ward Stay) 7]	as	End_Time_Ward_Stay_7	,
[Ward Code 8]	as	Ward_Code_8	,
[Location Class 8]	as	Location_Class_8	,
[Site Code (of Treatment) 8]	as	Site_Code_of_Treatment_8	,
[Start Date 8]	as	Start_Date_8	,
[Start Time (Ward Stay) 8]	as	Start_Time_Ward_Stay_8	,
[End Date 8]	as	End_Date_8	,
[End Time (Ward Stay) 8]	as	End_Time_Ward_Stay_8	,
[Ward Code 9]	as	Ward_Code_9	,
[Location Class 9]	as	Location_Class_9	,
[Site Code (of Treatment) 9]	as	Site_Code_of_Treatment_9	,
[Start Date 9]	as	Start_Date_9	,
[Start Time (Ward Stay) 9]	as	Start_Time_Ward_Stay_9	,
[End Date 9]	as	End_Date_9	,
[End Time (Ward Stay) 9]	as	End_Time_Ward_Stay_9	,
[Ward Code 10]	as	Ward_Code_10	,
[Location Class 10]	as	Location_Class_10	,
[Site Code (of Treatment) 10]	as	Site_Code_of_Treatment_10	,
[Start Date 10]	as	Start_Date_10	,
[Start Time (Ward Stay) 10]	as	Start_Time_Ward_Stay_10	,
[End Date 10]	as	End_Date_10	,
[End Time (Ward Stay) 10]	as	End_Time_Ward_Stay_10	,
[Ward Code 11]	as	Ward_Code_11	,
[Location Class 11]	as	Location_Class_11	,
[Site Code (of Treatment) 11]	as	Site_Code_of_Treatment_11	,
[Start Date 11]	as	Start_Date_11	,
[Start Time (Ward Stay) 11]	as	Start_Time_Ward_Stay_11	,
[End Date 11]	as	End_Date_11	,
[End Time (Ward Stay) 11]	as	End_Time_Ward_Stay_11	,
[Ward Code 12]	as	Ward_Code_12	,
[Location Class 12]	as	Location_Class_12	,
[Site Code (of Treatment) 12]	as	Site_Code_of_Treatment_12	,
[Start Date 12]	as	Start_Date_12	,
[Start Time (Ward Stay) 12]	as	Start_Time_Ward_Stay_12	,
[End Date 12]	as	End_Date_12	,
[End Time (Ward Stay) 12]	as	End_Time_Ward_Stay_12	,
[Ward Code 13]	as	Ward_Code_13	,
[Location Class 13]	as	Location_Class_13	,
[Site Code (of Treatment) 13]	as	Site_Code_of_Treatment_13	,
[Start Date 13]	as	Start_Date_13	,
[Start Time (Ward Stay) 13]	as	Start_Time_Ward_Stay_13	,
[End Date 13]	as	End_Date_13	,
[End Time (Ward Stay) 13]	as	End_Time_Ward_Stay_13	,
[Ward Code 14]	as	Ward_Code_14	,
[Location Class 14]	as	Location_Class_14	,
[Site Code (of Treatment) 14]	as	Site_Code_of_Treatment_14	,
[Start Date 14]	as	Start_Date_14	,
[Start Time (Ward Stay) 14]	as	Start_Time_Ward_Stay_14	,
[End Date 14]	as	End_Date_14	,
[End Time (Ward Stay) 14]	as	End_Time_Ward_Stay_14	,
[Ward Code 15]	as	Ward_Code_15	,
[Location Class 15]	as	Location_Class_15	,
[Site Code (of Treatment) 15]	as	Site_Code_of_Treatment_15	,
[Start Date 15]	as	Start_Date_15	,
[Start Time (Ward Stay) 15]	as	Start_Time_Ward_Stay_15	,
[End Date 15]	as	End_Date_15	,
[End Time (Ward Stay) 15]	as	End_Time_Ward_Stay_15	,
[Ward Code 16]	as	Ward_Code_16	,
[Location Class 16]	as	Location_Class_16	,
[Site Code (of Treatment) 16]	as	Site_Code_of_Treatment_16	,
[Start Date 16]	as	Start_Date_16	,
[Start Time (Ward Stay) 16]	as	Start_Time_Ward_Stay_16	,
[End Date 16]	as	End_Date_16	,
[End Time (Ward Stay) 16]	as	End_Time_Ward_Stay_16	,
[Ward Code 17]	as	Ward_Code_17	,
[Location Class 17]	as	Location_Class_17	,
[Site Code (of Treatment) 17]	as	Site_Code_of_Treatment_17	,
[Start Date 17]	as	Start_Date_17	,
[Start Time (Ward Stay) 17]	as	Start_Time_Ward_Stay_17	,
[End Date 17]	as	End_Date_17	,
[End Time (Ward Stay) 17]	as	End_Time_Ward_Stay_17	,
[Ward Code 18]	as	Ward_Code_18	,
[Location Class 18]	as	Location_Class_18	,
[Site Code (of Treatment) 18]	as	Site_Code_of_Treatment_18	,
[Start Date 18]	as	Start_Date_18	,
[Start Time (Ward Stay) 18]	as	Start_Time_Ward_Stay_18	,
[End Date 18]	as	End_Date_18	,
[End Time (Ward Stay) 18]	as	End_Time_Ward_Stay_18	,
[Ward Code 19]	as	Ward_Code_19	,
[Location Class 19]	as	Location_Class_19	,
[Site Code (of Treatment) 19]	as	Site_Code_of_Treatment_19	,
[Start Date 19]	as	Start_Date_19	,
[Start Time (Ward Stay) 19]	as	Start_Time_Ward_Stay_19	,
[End Date 19]	as	End_Date_19	,
[End Time (Ward Stay) 19]	as	End_Time_Ward_Stay_19	,
[Ward Code 20]	as	Ward_Code_20	,
[Location Class 20]	as	Location_Class_20	,
[Site Code (of Treatment) 20]	as	Site_Code_of_Treatment_20	,
[Start Date 20]	as	Start_Date_20	,
[Start Time (Ward Stay) 20]	as	Start_Time_Ward_Stay_20	,
[End Date 20]	as	End_Date_20	,
[End Time (Ward Stay) 20]	as	End_Time_Ward_Stay_20	,
[Ward Code 21]	as	Ward_Code_21	,
[Location Class 21]	as	Location_Class_21	,
[Site Code (of Treatment) 21]	as	Site_Code_of_Treatment_21	,
[Start Date 21]	as	Start_Date_21	,
[Start Time (Ward Stay) 21]	as	Start_Time_Ward_Stay_21	,
[End Date 21]	as	End_Date_21	,
[End Time (Ward Stay) 21]	as	End_Time_Ward_Stay_21	,
[Ward Code 22]	as	Ward_Code_22	,
[Location Class 22]	as	Location_Class_22	,
[Site Code (of Treatment) 22]	as	Site_Code_of_Treatment_22	,
[Start Date 22]	as	Start_Date_22	,
[Start Time (Ward Stay) 22]	as	Start_Time_Ward_Stay_22	,
[End Date 22]	as	End_Date_22	,
[End Time (Ward Stay) 22]	as	End_Time_Ward_Stay_22	,
[Ward Code 23]	as	Ward_Code_23	,
[Location Class 23]	as	Location_Class_23	,
[Site Code (of Treatment) 23]	as	Site_Code_of_Treatment_23	,
[Start Date 23]	as	Start_Date_23	,
[Start Time (Ward Stay) 23]	as	Start_Time_Ward_Stay_23	,
[End Date 23]	as	End_Date_23	,
[End Time (Ward Stay) 23]	as	End_Time_Ward_Stay_23	,
[Ward Code 24]	as	Ward_Code_24	,
[Location Class 24]	as	Location_Class_24	,
[Site Code (of Treatment) 24]	as	Site_Code_of_Treatment_24	,
[Start Date 24]	as	Start_Date_24	,
[Start Time (Ward Stay) 24]	as	Start_Time_Ward_Stay_24	,
[End Date 24]	as	End_Date_24	,
[End Time (Ward Stay) 24]	as	End_Time_Ward_Stay_24	,
[Ward Code at Episode End Date]	as	Ward_Code_at_Episode_End_Date	,
[Location Class at Episode End Date]	as	Location_Class_at_Episode_End_Date	,
[Site Code (of Treatment) at Episode End Date]	as	Site_Code_of_Treatment_at_Episode_End_Date					
from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
where 
[Ward Code 1]	 is not null
and   [Ward Code 1]<>''
)
select * from source_data