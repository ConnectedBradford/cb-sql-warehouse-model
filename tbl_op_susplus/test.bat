echo "Great day ahead"
echo "trying to start DBT for one model"

dbt run -m snapshot_diagnosis_icd

if %ERRORLEVEL% NEQ 0 (
    echo "Failed" 
    exit
) 
echo "Successful completion"

echo "hopefully it is done"

