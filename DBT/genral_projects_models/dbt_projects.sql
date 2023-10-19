{# Resource	        Description
models	            Each model lives in a single file and contains logic that either transforms raw data into a dataset that is ready for analytics or, more often, is an intermediate step in such a transformation.

snapshots	        A way to capture the state of your mutable tables so you can refer to it later.
seeds	            CSV files with static data that you can load into your data platform with dbt.
tests	            SQL queries that you can write to test the models and resources in your project.
macros	            Blocks of code that you can reuse multiple times.
docs	            Docs for your project that you can build.
sources	            A way to name and describe the data loaded into your warehouse by your Extract and Load tools.
exposures	        A way to define and describe a downstream use of your project.
metrics	            A way for you to define metrics for your project.
groups	            Groups enable collaborative node organization in restricted collections.
analysis	         A way to organize analytical SQL queries in your project such as the general ledger from your QuickBooks. #}

jaffle_shop
├── README.md
├── analyses
├── seeds
│   └── employees.csv
├── dbt_project.yml
├── macros
│   └── cents_to_dollars.sql
├── models
│   ├── intermediate
│   │   └── finance
│   │       ├── _int_finance__models.yml
│   │       └── int_payments_pivoted_to_orders.sql
│   ├── marts
│   │   ├── finance
│   │   │   ├── _finance__models.yml
│   │   │   ├── orders.sql
│   │   │   └── payments.sql
│   │   └── marketing
│   │       ├── _marketing__models.yml
│   │       └── customers.sql
│   ├── staging
│   │   ├── jaffle_shop
│   │   │   ├── _jaffle_shop__docs.md
│   │   │   ├── _jaffle_shop__models.yml
│   │   │   ├── _jaffle_shop__sources.yml
│   │   │   ├── base
│   │   │   │   ├── base_jaffle_shop__customers.sql
│   │   │   │   └── base_jaffle_shop__deleted_customers.sql
│   │   │   ├── stg_jaffle_shop__customers.sql
│   │   │   └── stg_jaffle_shop__orders.sql
│   │   └── stripe
│   │       ├── _stripe__models.yml
│   │       ├── _stripe__sources.yml
│   │       └── stg_stripe__payments.sql
│   └── utilities
│       └── all_dates.sql
├── packages.yml
├── snapshots
└── tests
    └── assert_positive_value_for_total_amount.sql