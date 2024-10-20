{{ config(
    materialized='incremental'
    ,pre_hook="truncate table {{ this }}"
    ) }}
with source_data as(
    select 
person_id	as	person_id			,
ID	as	ID			,
[Number of Babies]	as	Number_of_Babies			,
[First Antenatal Assessment Date]	as	First_Antenatal_Assessment_Date			,
[GMP (Code of GMP Responsible for Antenatal Care)]	as	GMP_Code_of_GMP_Responsible_for_Antenatal_Care			,
[Code of GP Practice (Registered GMP- Antenatal Care)]	as	Code_of_GP_Practice_Registered_GMP_Antenatal_Care			,
[Location Class of Delivery Place (Intended)]	as	Location_Class_of_Delivery_Place_Intended			,
[Delivery Place Change Reason]	as	Delivery_Place_Change_Reason			,
[Delivery Place Type (Intended)]	as	Delivery_Place_Type_Intended			,
[Anaesthetic Given During Labour Delivery]	as	Anaesthetic_Given_During_Labour_Delivery			,
[Gestation Length (Labour Onset)]	as	Gestation_Length_Labour_Onset			,
[Labour Delivery Onset Method]	as	Labour_Delivery_Onset_Method			,
[Delivery Date]	as	Delivery_Date			,
[Birth Order Baby 1]	as	Birth_Order_Baby_1			,
[Delivery Method Baby 1]	as	Delivery_Method_Baby_1			,
[Gestation Length (Assessment) Baby 1]	as	Gestation_Length_Assessment_Baby_1			,
[Status of Person Conducting Delivery Baby 1]	as	Status_of_Person_Conducting_Delivery_Baby_1			,
[NHS Number Status Indicator Baby 1]	as	NHS_Number_Status_Indicator_Baby_1			,
[Birth Weight Baby 1]	as	Birth_Weight_Baby_1			,
[Live or Still Birth Baby 1]	as	Live_or_Still_Birth_Baby_1			,
[Sex Baby 1]	as	Sex_Baby_1			,
[Location Class Delivery Place (Actual) 1]	as	Location_Class_Delivery_Place_Actual_1			,
[Delivery Place Type (Actual) 1]	as	Delivery_Place_Type_Actual_1			,
[Birth Order Baby 2]	as	Birth_Order_Baby_2			,
[Delivery Method Baby 2]	as	Delivery_Method_Baby_2			,
[Gestation Length (Assessment) Baby 2]	as	Gestation_Length_Assessment_Baby_2			,
[Status of Person Conducting Delivery Baby 2]	as	Status_of_Person_Conducting_Delivery_Baby_2			,
[Organisation Code Local Patient ID Baby 2]	as	Organisation_Code_Local_Patient_ID_Baby_2			,
[NHS Number Status Indicator Baby 2]	as	NHS_Number_Status_Indicator_Baby_2			,
[Birth Weight Baby 2]	as	Birth_Weight_Baby_2			,
[Live or Still Birth Baby 2]	as	Live_or_Still_Birth_Baby_2			,
[Sex Baby 2]	as	Sex_Baby_2			,
[Location Class Delivery Place (Actual) 2]	as	Location_Class_Delivery_Place_Actual_2			,
[Delivery Place Type (Actual) 2]	as	Delivery_Place_Type_Actual_2			,
[Birth Order Baby 3]	as	Birth_Order_Baby_3			,
[Delivery Method Baby 3]	as	Delivery_Method_Baby_3			,
[Gestation Length (Assessment) Baby 3]	as	Gestation_Length_Assessment_Baby_3			,
[Status of Person Conducting Delivery Baby 3]	as	Status_of_Person_Conducting_Delivery_Baby_3			,
[Organisation Code Local Patient ID Baby 3]	as	Organisation_Code_Local_Patient_ID_Baby_3			,
[NHS Number Status Indicator Baby 3]	as	NHS_Number_Status_Indicator_Baby_3			,
[Birth Weight Baby 3]	as	Birth_Weight_Baby_3			,
[Live or Still Birth Baby 3]	as	Live_or_Still_Birth_Baby_3			,
[Location Class Delivery Place (Actual) 3]	as	Location_Class_Delivery_Place_Actual_3			,
[Delivery Place Type (Actual) 3]	as	Delivery_Place_Type_Actual_3			,
[Birth Order Baby 4]	as	Birth_Order_Baby_4			,
[Delivery Method Baby 4]	as	Delivery_Method_Baby_4			,
[Gestation Length (Assessment) Baby 4]	as	Gestation_Length_Assessment_Baby_4			,
[Status of Person Conducting Delivery Baby 4]	as	Status_of_Person_Conducting_Delivery_Baby_4			,
[Organisation Code Local Patient ID Baby 4]	as	Organisation_Code_Local_Patient_ID_Baby_4			,
[NHS Number Status Indicator Baby 4]	as	NHS_Number_Status_Indicator_Baby_4			,
[Birth Weight Baby 4]	as	Birth_Weight_Baby_4			,
[Live or Still Birth Baby 4]	as	Live_or_Still_Birth_Baby_4			,
[Location Class Delivery Place (Actual) 4]	as	Location_Class_Delivery_Place_Actual_4			,
[Delivery Place Type (Actual) 4]	as	Delivery_Place_Type_Actual_4			,
[Birth Order Baby 5]	as	Birth_Order_Baby_5			,
[Delivery Method Baby 5]	as	Delivery_Method_Baby_5			,
[Gestation Length (Assessment) Baby 5]	as	Gestation_Length_Assessment_Baby_5			,
[Status of Person Conducting Delivery Baby 5]	as	Status_of_Person_Conducting_Delivery_Baby_5			,
[Organisation Code Local Patient ID Baby 5]	as	Organisation_Code_Local_Patient_ID_Baby_5			,
[NHS Number Status Indicator Baby 5]	as	NHS_Number_Status_Indicator_Baby_5			,
[Birth Weight Baby 5]	as	Birth_Weight_Baby_5			,
[Live or Still Birth Baby 5]	as	Live_or_Still_Birth_Baby_5			,
[Location Class Delivery Place (Actual) 5]	as	Location_Class_Delivery_Place_Actual_5			,
[Delivery Place Type (Actual) 5]	as	Delivery_Place_Type_Actual_5			,
[Birth Order Baby 6]	as	Birth_Order_Baby_6			,
[Delivery Method Baby 6]	as	Delivery_Method_Baby_6			,
[Gestation Length (Assessment) Baby 6]	as	Gestation_Length_Assessment_Baby_6			,
[Status of Person Conducting Delivery Baby 6]	as	Status_of_Person_Conducting_Delivery_Baby_6			,
[Organisation Code Local Patient ID Baby 6]	as	Organisation_Code_Local_Patient_ID_Baby_6			,
[NHS Number Status Indicator Baby 6]	as	NHS_Number_Status_Indicator_Baby_6			,
[Birth Weight Baby 6]	as	Birth_Weight_Baby_6			,
[Live or Still Birth Baby 6]	as	Live_or_Still_Birth_Baby_6			,
[Location Class Delivery Place (Actual) 6]	as	Location_Class_Delivery_Place_Actual_6			,
[Delivery Place Type (Actual) 6]	as	Delivery_Place_Type_Actual_6			,
[Birth Order Baby 7]	as	Birth_Order_Baby_7			,
[Delivery Method Baby 7]	as	Delivery_Method_Baby_7			,
[Gestation Length (Assessment) Baby 7]	as	Gestation_Length_Assessment_Baby_7			,
[Status of Person Conducting Delivery Baby 7]	as	Status_of_Person_Conducting_Delivery_Baby_7			,
[Organisation Code Local Patient ID Baby 7]	as	Organisation_Code_Local_Patient_ID_Baby_7			,
[NHS Number Status Indicator Baby 7]	as	NHS_Number_Status_Indicator_Baby_7			,
[Birth Weight Baby 7]	as	Birth_Weight_Baby_7			,
[Live or Still Birth Baby 7]	as	Live_or_Still_Birth_Baby_7			,
[Location Class Delivery Place (Actual) 7]	as	Location_Class_Delivery_Place_Actual_7			,
[Delivery Place Type (Actual) 7]	as	Delivery_Place_Type_Actual_7			,
[Birth Order Baby 8]	as	Birth_Order_Baby_8			,
[Delivery Method Baby 8]	as	Delivery_Method_Baby_8			,
[Gestation Length (Assessment) Baby 8]	as	Gestation_Length_Assessment_Baby_8			,
[Status of Person Conducting Delivery Baby 8]	as	Status_of_Person_Conducting_Delivery_Baby_8			,
[Organisation Code Local Patient ID Baby 8]	as	Organisation_Code_Local_Patient_ID_Baby_8			,
[NHS Number Status Indicator Baby 8]	as	NHS_Number_Status_Indicator_Baby_8			,
[Birth Weight Baby 8]	as	Birth_Weight_Baby_8			,
[Live or Still Birth Baby 8]	as	Live_or_Still_Birth_Baby_8			,
[Location Class Delivery Place (Actual) 8]	as	Location_Class_Delivery_Place_Actual_8			,
[Delivery Place Type (Actual) 8]	as	Delivery_Place_Type_Actual_8			,
[Birth Order Baby 9]	as	Birth_Order_Baby_9			,
[Delivery Method Baby 9]	as	Delivery_Method_Baby_9			,
[Gestation Length (Assessment) Baby 9]	as	Gestation_Length_Assessment_Baby_9			,
[Status of Person Conducting Delivery Baby 9]	as	Status_of_Person_Conducting_Delivery_Baby_9			,
[Organisation Code Local Patient ID Baby 9]	as	Organisation_Code_Local_Patient_ID_Baby_9			,
[NHS Number Status Indicator Baby 9]	as	NHS_Number_Status_Indicator_Baby_9			,
[Birth Weight Baby 9]	as	Birth_Weight_Baby_9			,
[Live or Still Birth Baby 9]	as	Live_or_Still_Birth_Baby_9			,
[Location Class Delivery Place (Actual) 9]	as	Location_Class_Delivery_Place_Actual_9			,
[Delivery Place Type (Actual) 9]	as	Delivery_Place_Type_Actual_9			,
[Gestation Length (Assessment) Baby]	as	Gestation_Length_Assessment_Baby			,
[Organisation Code (Local Patient Identifier (Mother))]	as	Organisation_Code_Local_Patient_Identifier_Mother			,
[NHS Number Status Indicator (Mother)]	as	NHS_Number_Status_Indicator_Mother			,
[Postcode of Mother]	as	Postcode_of_Mother			,
[Organisation Code (PCT of Residence) (Mother)]	as	Organisation_Code_PCT_of_Residence_Mother			
from {{ source('warehouse', 'tbl_apc_finished_susplus') }}
where [Delivery Date] is  not null and  [Delivery Date] <>''
)
select * from source_data