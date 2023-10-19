# https://docs.getdbt.com/docs/build/python-models

'''dbt Python (dbt-py) models can help you solve use cases that can't be solved with SQL. You can perform analyses using tools available in the open-source Python ecosystem, including state-of-the-art packages for data science and statistics. Before, you would have needed separate infrastructure and orchestration to run Python transformations in production. Python transformations defined in dbt are models in your project with all the same capabilities around testing, documentation, and lineage.'''

# models/my_python_model.py
import ...

def model(dbt, session):

    my_sql_model_df = dbt.ref("my_sql_model")

    final_df = ...  # stuff you can't write in SQL!

    return final_df

'''
# models/config.yml
version: 2

models:
  - name: my_python_model

    # Document within the same codebase
    description: My transformation written in Python

    # Configure in ways that feel intuitive and familiar
    config:
      materialized: table
      tags: ['python']

    # Test the results of my Python transformation
    columns:
      - name: id
        # Standard validation for 'grain' of Python results
        tests:
          - unique
          - not_null
    tests:
      # Write your own validation logic (in SQL) for Python results
      - custom_generic_test
'''


# Referencing other models
# models/my_python_model.py

def model(dbt, session):

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("upstream_model_name")

    # DataFrame representing an upstream source
    upstream_source = dbt.source("upstream_source_name", "table_name")

    ...


'''you can ref() your Python model in downstream SQL models, too:'''
    with upstream_python_model as (

    select * from {{ ref('my_python_model') }}

),
    

# Configuring Python models
def model(dbt, session):
    # Setting the Configuration
   dbt.config(materialized="table")

'''==========================================================================================================================='''
# models/config.yml
'''
version: 2

models:
  - name: my_python_model
    config:
      materialized: table
      target_name: "{{ target.name }}"
      specific_var: "{{ var('SPECIFIC_VAR') }}"
      specific_env_var: "{{ env_var('SPECIFIC_ENV_VAR') }}"
'''

def model(dbt, session):
    target_name = dbt.config.get("target_name")
    specific_var = dbt.config.get("specific_var")
    specific_env_var = dbt.config.get("specific_env_var")

    orders_df = dbt.ref("fct_orders")

    # limit data in dev
    if target_name == "dev":
        orders_df = orders_df.limit(500)

'''==========================================================================================================================='''
# models/my_python_model.py

# Snowpark Examples
# 1. Incremental

import snowflake.snowpark.functions as F
def model(dbt, session):
    dbt.config(materialized = "incremental")
    df = dbt.ref("upstream_table")

    if dbt.is_incremental:

        # only new rows compared to max in current table
        max_from_this = f"select max(updated_at) from {dbt.this}"
        df = df.filter(df.updated_at >= session.sql(max_from_this).collect()[0][0])

        # or only rows from the past 3 days
        df = df.filter(df.updated_at >= F.dateadd("day", F.lit(-3), F.current_timestamp()))

    ...

    return df

# 2. Using PyPI packages
import holidays

def is_holiday(date_col):
    # Chez Jaffle
    french_holidays = holidays.France()
    is_holiday = (date_col in french_holidays)
    return is_holiday

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["holidays"]
    )

    orders_df = dbt.ref("stg_orders")

    df = orders_df.to_pandas()

    # apply our function
    # (columns need to be in uppercase on Snowpark)
    df["IS_HOLIDAY"] = df["ORDER_DATE"].apply(is_holiday)
    df["ORDER_DATE"].dt.tz_localize('UTC') # convert from Number/Long to tz-aware Datetime

    # return final dataset (Pandas DataFrame)
    return df


# 3. UDF's
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import numpy

def register_udf_add_random():
    add_random = F.udf(
        # use 'lambda' syntax, for simple functional behavior
        lambda x: x + numpy.random.normal(),
        return_type=T.FloatType(),
        input_types=[T.FloatType()]
    )
    return add_random

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["numpy"]
    )

    temps_df = dbt.ref("temperatures")

    add_random = register_udf_add_random()

    # warm things up, who knows by how much
    df = temps_df.withColumn("degree_plus_random", add_random("degree"))
    return df




# Pyspark Examples
# 1. Incremental

import pyspark.sql.functions as F

def model(dbt, session):
    dbt.config(materialized = "incremental")
    df = dbt.ref("upstream_table")

    if dbt.is_incremental:

        # only new rows compared to max in current table
        max_from_this = f"select max(updated_at) from {dbt.this}"
        df = df.filter(df.updated_at >= session.sql(max_from_this).collect()[0][0])

        # or only rows from the past 3 days
        df = df.filter(df.updated_at >= F.date_add(F.current_timestamp(), F.lit(-3)))

    ...

    return df


# 2. Using PyPI packages
import holidays

def is_holiday(date_col):
    # Chez Jaffle
    french_holidays = holidays.France()
    is_holiday = (date_col in french_holidays)
    return is_holiday

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["holidays"]
    )

    orders_df = dbt.ref("stg_orders")

    df = orders_df.to_pandas_on_spark()  # Spark 3.2+
    # df = orders_df.toPandas() in earlier versions

    # apply our function
    df["is_holiday"] = df["order_date"].apply(is_holiday)

    # convert back to PySpark
    df = df.to_spark()               # Spark 3.2+
    # df = session.createDataFrame(df) in earlier versions

    # return final dataset (PySpark DataFrame)
    return df


# 3. UDF's
import pyspark.sql.types as T
import pyspark.sql.functions as F
import numpy

# use a 'decorator' for more readable code
@F.udf(returnType=T.DoubleType())
def add_random(x):
    random_number = numpy.random.normal()
    return x + random_number

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["numpy"]
    )

    temps_df = dbt.ref("temperatures")

    # warm things up, who knows by how much
    df = temps_df.withColumn("degree_plus_random", add_random("degree"))
    return df


# =============================================================================================================================================
# Defining functions
def add_one(x):
    return x + 1

def model(dbt, session):
    dbt.config(materialized="table")
    temps_df = dbt.ref("temperatures")

    # warm things up just a little
    df = temps_df.withColumn("degree_plus_one", add_one(temps_df["degree"]))
    return df


# Configuring package

# models/my_python_model.py

def model(dbt, session):
    dbt.config(
        packages = ["numpy==1.23.1", "scikit-learn"]
    )

# models/config.yml
'''
version: 2

models:
  - name: my_python_model
    config:
      packages:
        - "numpy==1.23.1"
        - scikit-learn
        '''