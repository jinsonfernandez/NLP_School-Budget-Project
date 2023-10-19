{# https://docs.getdbt.com/docs/build/snapshots #}
{# SCD2: https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row #}


{# Analysts often need to "look back in time" at previous data states in their mutable tables. While some source data systems are built in a way that makes accessing historical data possible, this is not always the case. dbt provides a mechanism, snapshots, which records changes to a mutable table over time.

Snapshots implement type-2 Slowly Changing Dimensions over mutable source tables. These Slowly Changing Dimensions (or SCDs) identify how a row in a table changes over time. Imagine you have an orders table where the status field can be overwritten as the order is processed. #}

id	     status	            updated_at
1	     pending	        2019-01-01

Now, imagine that the order goes from "pending" to "shipped". That same record will now look like:

id	     status	            updated_at
1	     shipped	         2019-01-02

{# dbt can "snapshot" these changes to help you understand how values in a row change over time. Here's an example of a snapshot table for the previous example: #}

id	status	                    updated_at	                 dbt_valid_from	                dbt_valid_to
1	pending	                     2019-01-01	                  2019-01-01	                 2019-01-02
1	shipped	                     2019-01-02	                  2019-01-02	                    null



{# In dbt, snapshots are select statements, defined within a snapshot block in a .sql file (typically in your snapshots directory). You'll also need to configure your snapshot to tell dbt how to detect record changes. #}

{# snapshots/orders_snapshot.sql #}
{% snapshot orders_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}

{# When you run the dbt snapshot command:

On the first run: dbt will create the initial snapshot table — this will be the result set of your select statement, with additional columns including dbt_valid_from and dbt_valid_to. All records will have a dbt_valid_to = null.
On subsequent runs: dbt will check which records have changed or if any new records have been created:
The dbt_valid_to column will be updated for any existing records that have changed
The updated record and any new records will be inserted into the snapshot table. These records will now have dbt_valid_to = null #}



===============================================================================================================================
Example
To add a snapshot to your project:

Create a file in your snapshots directory with a .sql file extension, e.g. snapshots/orders.sql
Use a snapshot block to define the start and end of a snapshot:

snapshots/orders_snapshot.sql
{% snapshot orders_snapshot %}

{% endsnapshot %}

Write a select statement within the snapshot block (tips for writing a good snapshot query are below). 
This select statement defines the results that you want to snapshot over time. You can use sources and refs here.

snapshots/orders_snapshot.sql
{% snapshot orders_snapshot %}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}

Check whether the result set of your query includes a reliable timestamp column that indicates when a record was last updated. For our example, the updated_at column reliably indicates record changes, so we can use the timestamp strategy. If your query result set does not have a reliable timestamp, you'll need to instead use the check strategy — more details on this below.

Add configurations to your snapshot using a config block (more details below). You can also configure your snapshot from your dbt_project.yml file (docs).

snapshots/orders_snapshot.sql
{% snapshot orders_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}

==============================================================================================================================

Detecting row changes

Snapshot "strategies" define how dbt knows if a row has changed. There are two strategies built-in to dbt — timestamp and check.

1.Timestamp strategy (recommended)

The timestamp strategy uses an updated_at field to determine if a row has changed. If the configured updated_at column for a row is more recent than the last time the snapshot ran, then dbt will invalidate the old record and record the new one. If the timestamps are unchanged, then dbt will not take any action.

The timestamp strategy requires the following configurations:

Config	Description	Example
updated_at	A column which represents when the source row was last updated	updated_at
Example usage:

snapshots/orders_snapshot_timestamp.sql
{% snapshot orders_snapshot_timestamp %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at',
        )
    }}

    select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}





2. Check strategy
The check strategy is useful for tables which do not have a reliable updated_at column. This strategy works by comparing a list of columns between their current and historical values. If any of these columns have changed, then dbt will invalidate the old record and record the new one. If the column values are identical, then dbt will not take any action.

The check strategy requires the following configurations:

Config	Description	Example
check_cols	A list of columns to check for changes, or all to check all columns	["name", "email"]
CHECK_COLS = 'ALL'
The check snapshot strategy can be configured to track changes to all columns by supplying check_cols = 'all'. It is better to explicitly enumerate the columns that you want to check. Consider using a surrogate key to condense many columns into a single column.

{% snapshot orders_snapshot_check %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='id',
          check_cols=['status', 'is_cancelled'],
        )
    }}

    select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}

================================================================================================================================
Hard deletes (opt-in)

Rows that are deleted from the source query are not invalidated by default. With the config option invalidate_hard_deletes, dbt can track rows that no longer exist. This is done by left joining the snapshot table with the source table, and filtering the rows that are still valid at that point, but no longer can be found in the source table. dbt_valid_to will be set to the current snapshot time.

This configuration is not a different strategy as described above, but is an additional opt-in feature. It is not enabled by default since it alters the previous behavior.

For this configuration to work with the timestamp strategy, the configured updated_at column must be of timestamp type. Otherwise, queries will fail due to mixing data types.

Example Usage

snapshots/orders_snapshot_hard_delete.sql
{% snapshot orders_snapshot_hard_delete %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at',
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}


================================================================================================================================
Configuring snapshots
Snapshot configurations
There are a number of snapshot-specific configurations:

Config	                Description	                             Required?	                         Example
target_database	        The database that dbt should 
                        render the snapshot table into	            No	                              analytics

target_schema	        The schema that dbt should render 
                        the snapshot table into	                    Yes	                              snapshots

strategy	            The snapshot strategy to use. 
                        One of timestamp or check	                Yes	                               timestamp

unique_key	            A primary key column or expression 
                        for the record	                            Yes	                                id

check_cols	            If using the check strategy, then the 
                        columns to check	                        Only if using the check strategy	 ["status"]

updated_at	            If using the timestamp strategy, 
                        the timestamp column to compare	            Only if using the timestamp strategy	updated_at

invalidate_hard_deletes	Find hard deleted records in source, 
                        and set dbt_valid_to current time if no longer exists	No	                        True




Snapshots can be configured from both your dbt_project.yml file and a config block, check out the configuration docs for more information.

Note: BigQuery users can use target_project and target_dataset as aliases for target_database and target_schema, respectively.

Configuration best practices
Use the timestamp strategy where possible
This strategy handles column additions and deletions better than the check strategy.

Ensure your unique key is really unique
The unique key is used by dbt to match rows up, so it's extremely important to make sure this key is actually unique! If you're snapshotting a source, I'd recommend adding a uniqueness test to your source (example).

Use a target_schema that is separate to your analytics schema
Snapshots cannot be rebuilt. As such, it's a good idea to put snapshots in a separate schema so end users know they are special. From there, you may want to set different privileges on your snapshots compared to your models, and even run them as a different user (or role, depending on your warehouse) to make it very difficult to drop a snapshot unless you really want to.


===============================================================================================================================
Snapshot query best practices

Snapshot source data.
Your models should then select from these snapshots, treating them like regular data sources. As much as possible, snapshot your source data in its raw form and use downstream models to clean up the data

Use the source function in your query.
This helps when understanding data lineage in your project.

Include as many columns as possible.
In fact, go for select * if performance permits! Even if a column doesn't feel useful at the moment, it might be better to snapshot it in case it becomes useful – after all, you won't be able to recreate the column later.

Avoid joins in your snapshot query.
Joins can make it difficult to build a reliable updated_at timestamp. Instead, snapshot the two tables separately, and join them in downstream models.

Limit the amount of transformation in your query.
If you apply business logic in a snapshot query, and this logic changes in the future, it can be impossible (or, at least, very difficult) to apply the change in logic to your snapshots.

Basically – keep your query as simple as possible! Some reasonable exceptions to these recommendations include:

Selecting specific columns if the table is wide.
Doing light transformation to get data into a reasonable shape, for example, unpacking a JSON blob to flatten your source data into columns.

=============================================================================================================================
Snapshot meta-fields

Field	                Meaning	                                                Usage
dbt_valid_from	        The timestamp when 
                        this snapshot row was first inserted	                This column can be used to order the 
                                                                                different "versions" of a record.


dbt_valid_to	        The timestamp when this row became invalidated.	        The most recent snapshot record will have 
                                                                                dbt_valid_to set to null.

dbt_scd_id	            A unique key generated for each snapshotted record.	    This is used internally by dbt

dbt_updated_at	        The updated_at timestamp of the source record 
                        when this snapshot row was inserted.	                This is used internally by dbt
================================================================================================================================
1.  How do I run one snapshot at a time?
$ dbt snapshot --select order_snapshot

2. How often should I run the snapshot command?
Snapshots are a batch-based approach to change data capture. The dbt snapshot command must be run on a schedule to ensure that changes to tables are actually recorded! While individual use-cases may vary, snapshots are intended to be run between hourly and daily. If you find yourself snapshotting more frequently than that, consider if there isn't a more appropriate way to capture changes in your source data tables.

3.  What happens if I add new columns to my snapshot query?
When the columns of your source query changes, dbt will attempt to reconcile this change in the destination snapshot table. dbt does this by:

Creating new columns from the source query in the destination table
Expanding the size of string types where necessary (eg. varchars on Redshift)
dbt will not delete columns in the destination snapshot table if they are removed from the source query. It will also not change the type of a column beyond expanding the size of varchar columns. That is, if a string column is changed to a date column in the snapshot source query, dbt will not attempt to change the type of the column in the destination table.


4.  Do hooks run with snapshots?
Yes! The following hooks are available for snapshots:

pre-hooks
post-hooks
on-run-start
on-run-end


5. Why is there only one `target_schema` for snapshots?
Snapshots build into the same target_schema, no matter who is running them.

In comparison, models build into a separate schema for each user — this helps maintain separate development and production environments.

So, why the difference?

Let's assume you are running your snapshot regularly. If the model had a different target in dev (e.g. dbt_claire) compared to prod (e.g. analytics), when you ref the model in dev, dbt would select from a snapshot that has not been run regularly. This can make it hard to build models since the data differs from prod.

Instead, in the models that ref your snapshots, it makes more sense to select from the production version of your snapshot, even when developing models. In this way, snapshot tables are more similar to source data than they are to proper dbt models.

For this reason, there is only one target_schema, which is not environment-aware by default.

However, this can create problems if you need to run a snapshot command when developing your models, or during a CI run. Fortunately, there's a few workarounds — check out this forum article. https://discourse.getdbt.com/t/using-dynamic-schemas-for-snapshots/1070
