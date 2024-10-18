{% macro get_run_date() %}
  {% if execute %}
    {% set run_date = run_query("select max(run_date) as RUN_DT from {{ source('warehouse', 'tbl_apc_finished_susplus') }}E where DB_NAME='WAREHOUSE'").columns[0][0] %}
  {% else %}  
    {% set last_audit_id = -1 %}
  {% endif %}

  {% do return(last_audit_id) %}
{% endmacro %}