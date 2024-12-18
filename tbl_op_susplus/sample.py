cd C:\Project
dbt-env\Scripts\activate

cd C:\Project\dbt_proj\tbl_op_susplus

from dbt.task.run import RunTask
from dbt.config import RuntimeConfig

# Configure your dbt project
config = RuntimeConfig.from_args(args)
task = RunTask(config)

# Execute dbt run
results = task.run()
print(results)