import json
import os
from dagster import file_relative_path, ScheduleDefinition
from dagster_dbt import load_assets_from_dbt_manifest


DBT_PROJECT_PATH = file_relative_path(__file__, "../../jaffle_shop")

with open(os.path.join(DBT_PROJECT_PATH, "target", "manifest.json")) as f:
    manifest_json = json.load(f)

dbt_assets = load_assets_from_dbt_manifest(manifest_json, key_prefix="stone")
