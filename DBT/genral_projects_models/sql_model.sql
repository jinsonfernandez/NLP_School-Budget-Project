-- https://docs.getdbt.com/docs/build/sql-models

{# A SQL model is a select statement. Models are defined in .sql files (typically in your models directory):

Each .sql file contains one model / select statement
The model name is inherited from the filename.
Models can be nested in subdirectories within the models directory
When you execute the dbt run command, dbt will build this model data warehouse by wrapping it in a create view as or create table as statement. #}

{# models/customers.sql #}
with customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from jaffle_shop.orders

    group by 1
)

select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customer_orders.first_order_date,
    customer_orders.most_recent_order_date,
    coalesce(customer_orders.number_of_orders, 0) as number_of_orders

from jaffle_shop.customers

left join customer_orders using (customer_id)


{#  How can I see the SQL that dbt is running? #}
To check out the SQL that dbt is running, you can look in:

dbt Cloud:
Within the run output, click on a model name, and then select "Details"
dbt CLI:
The target/compiled/ directory for compiled select statements
The target/run/ directory for compiled create statements
The logs/dbt.log file for verbose logging.

{# =====================================================================================================================================
Configuring models #}

Configurations are "model settings" that can be set in your dbt_project.yml file, and in your model file using a config block. Some example configurations include:

Changing the materialization that a model uses — a materialization determines the SQL that dbt uses to create the model in your warehouse.
Build models into separate schemas.
Apply tags to a model.

{# dbt_project.yml #}
name: jaffle_shop
config-version: 2
...

models:
  jaffle_shop: # this matches the `name:`` config
    +materialized: view # this applies to all models in the current project
    marts:
      +materialized: table # this applies to all models in the `marts/` directory
      marketing:
        +schema: marketing # this applies to all models in the `marts/marketing/`` directory

{# models/customers.sql #}
{{ config(
    materialized="view",
    schema="marketing"
) }}

with customer_orders as ...

{# ===========================================================================================================================================
Building dependencies between models #}

You can build dependencies between models by using the ref function in place of table names in a query. Use the name of another model as the argument for ref.

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

...
============================================================================================================================================
Can I build my models in a schema other than my target schema or split my models across multiple schemas?
Yes! Use the schema configuration in your dbt_project.yml file, or using a config block:

dbt_project.yml
name: jaffle_shop
...

models:
  jaffle_shop:
    marketing:
      schema: marketing # seeds in the `models/mapping/ subdirectory will use the marketing schema

models/customers.sql
{{
  config(
    schema='core'
  )
}}

How do I specify column types?
select
    id,
    created::timestamp as created
from some_other_table