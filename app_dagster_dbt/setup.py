from setuptools import find_packages, setup

setup(
    name="dbt_dagster",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "pandas",
        "dbt-core",
        "dbt-bigquery",
        "dbt-duckdb",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "plotly",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
