from dagster import Definitions, load_assets_from_modules, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCli

from dbt_dagster import assets
from dbt_dagster.assets import DBT_PROJECT_PATH

resources = {
    "dbt": DbtCli(project_dir=DBT_PROJECT_PATH),
}

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=resources,
    schedules=[
        ScheduleDefinition(
            job=define_asset_job("run_everything", selection="*"),
            cron_schedule="@daily",
        )
    ]
)
