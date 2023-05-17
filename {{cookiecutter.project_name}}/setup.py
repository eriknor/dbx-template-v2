"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from {{cookiecutter.project_slug}} import __version__

PACKAGE_REQUIREMENTS = ["great_expectations","pyyaml"]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark==3.2.1",
    "delta-spark==1.1.0",
    "scikit-learn",
    "pandas",
    "mlflow",
    #"great_expectations",
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.8"
]

setup(
    name="{{cookiecutter.project_slug}}",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools","wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    entry_points = {
        "console_scripts": [
            "{{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.sqlTask = {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.tasks.sample_etl_task:entrypoint",
            "{{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.pythonTask = {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.tasks.sample_python_task:entrypoint",
            "{{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.expectationsTask = {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.tasks.sample_great_expectations:entrypoint",
    ]},
    version=__version__,
    description="",
    author="",
    # package_dir={"{{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}": "{{cookiecutter.project_slug}}/{{cookiecutter.pipeline_slug}}"},
    package_data={'': ['resources/sql/*.sql']},
)
