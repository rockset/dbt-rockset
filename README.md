# dbt-rockset

The dbt-Rockset adapter brings real-time analytics to [dbt](https://www.getdbt.com/). Using the adapter, you can load data into [Rockset](https://rockset.com/) and create collections, by writing SQL SELECT statements in dbt. These collections can then be built on top of each other to support highly-complex data transformations with many dependency edges.

The following subsections describe the adapter's installation procedure and support for dbt:

* [Installation and Set up](#installation-and-set-up)
* [Supported Materializations](#supported-materializations)
* [Real-Time Streaming ELT Using dbt + Rockset](#real-time-streaming-elt-using-dbt--rockset)
* [Persistent Materializations Using dbt + Rockset](#persistent-materializations-using-dbt--rockset)
* [Testing, Formatting, & Caveats](#testing-formatting--caveats)

See the following blogs for additional information:
* [Real-Time Analytics with dbt + Rockset](https://rockset.com/blog/real-time-analytics-with-dbt-rockset/)
* [Real-Time Data Transformations with dbt + Rockset](https://rockset.com/blog/real-time-data-transformations-dbt-rockset/).

## Installation and Set up

The following subsections describe how to set up and use the adapter:

* [Install the Plug-in](#install-the-plug-in)
* [Configure your Profile](#configure-your-profile)

See the [adapter's GitHub repo](https://github.com/rockset/dbt-rockset) for additional information.

### Install the Plug-in

Open a command-line window and run the following command to install the adapter:

```bash
pip3 install dbt-rockset
```

### Configure your Profile

Configure a [dbt profile](https://docs.getdbt.com/dbt-cli/configure-your-profile) similar to the example shown below, to connect with your Rockset account. Enter any workspace that you’d like your dbt collections to be created in, and any Rockset API key. The database field is required by dbt but unused in Rockset.

```
rockset:
  outputs:
    dev:
      type: rockset
      threads: 1
      database: N/A
      workspace: <rockset_workspace_name>
      api_key: <rockset_api_key>
      api_server: <rockset_api_server> # Optional, default is `api.usw2a1.rockset.com`, the api server for region Oregon.
      vi_rrn: <rockset_virtual_instance_rrn> # Optional, the VI to use for IIS queries
      run_async_iis: <async_iis_queries> # Optional, by default false, whether use async execution for IIS queries
  target: dev
```

Update your dbt project to use this Rockset dbt profile. You can switch profiles in your project by editing the ```dbt_project.yml``` file.

## Supported Materializations

Type | Supported? | Details
-----|------------|----------------
[Table](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#table) | YES | Creates a [Rockset collection](https://docs.rockset.com/collections/).
[View](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#view) | YES | Creates a [Rockset view](https://rockset.com/docs/views/#gatsby-focus-wrapper).
[Ephemeral](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#ephemeral) | Yes | Create a CTE.
[Incremental](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#incremental) | YES | Creates a [Rockset collection](https://docs.rockset.com/collections/) if it doesn't exist, and writes to it.

## Real-Time Streaming ELT Using dbt + Rockset

As data is ingested, Rockset performs the following:
* The data is automatically indexed in at least three different ways using Rockset’s [Converged Index™](https://rockset.com/blog/converged-indexing-the-secret-sauce-behind-rocksets-fast-queries/) technology.
* Your write-time data transformations are performed.
* The data is made available for queries within seconds.

When you execute queries on that data, Rockset leverages those indexes to complete any read-time data transformations you define using dbt, with sub-second latency.

### Write-Time Data Transformations Using Rollups and Ingest Transformation 

Rockset can extract and load semi-structured data from multiple sources in real-time. 
For high-velocity data (e.g. data streams), you can roll it up at write-time. For example, when you have streaming data coming in from Kafka or Kinesis,
you can create a Rockset collection for each data stream, and then set up [rollups](https://rockset.com/blog/how-rockset-enables-sql-based-rollups-for-streaming-data/), to perform transformations and aggregations on the data as it is written into Rockset. This can help to:

* Reduce the size of large scale data streams.
* De-duplicate data.
* Partition your data.

Collections can also be created from other data sources including:
* Data lakes (e.g., S3 or GCS).
* NoSQL databases (e.g., DynamoDB or MongoDB)
* Relational databases (e.g., PostgreSQL or MySQL).

You can then use Rocket’s [ingest transformation](/ingest-transformation) to transform the data using SQL statements as it is written into Rockset.

### Read-Time Data Transformations Using Rockset Views

The adapter can set up data transformations as SQL statements in dbt, using View Materializations that can be performed during read-time.

To set this up:

1. Create a dbt model using SQL statements for each transformation you want to perform on your data.
2. Execute ```dbt run```. dbt will automatically create a Rockset View for each dbt model, which performs all the data transformations when queries are executed.

If queries complete within your latency requirements, then you have achieved the gold standard of real-time data transformations: Real-Time Streaming ELT.

Your data will be automatically kept up-to-date in real-time, and reflected in your queries. There is no need for periodic batch updates to “refresh” your data. You will not need to execute ```dbt run``` again after the initial set up, unless you want to make changes to the actual data transformation logic (e.g. adding or updating dbt models).

## Persistent Materializations Using dbt + Rockset

If write-time transformations and views don't meet your application’s latency requirements (or your data transformations become too complex), you can persist them as Rockset collections.

Rockset requires queries to complete in under two minutes to cater to real-time use cases, which may affect you if your read-time transformations are too complicated. This requires a batch ELT workflow to manually execute ```dbt run``` each time you want to update your data transformations. You can use micro-batching to frequently run dbt, to keep your transformed data up-to-date in near real-time.

Persistent materializations are both faster to query and better at handling query concurrency, as they are materialized as collections in Rockset. Since the bulk of the data transformations have already been performed ahead of time, your queries will complete significantly faster because you can minimize the complexity necessary during read-time.

There are two persistent materializations available in dbt: 

* [Incremental Models](#materializing-dbt-incremental-models-in-rockset)
* [Table Models](#materializing-dbt-table-models-in-rockset)

### Materializing dbt Incremental Models in Rockset

[Incremental Models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#incremental) enable you to insert or update documents into a Rockset collection since the last time dbt was run. This can significantly reduce the build time since Rockset only needs to perform transformations on the new data that was just generated, rather than dropping, recreating, and performing transformations on the entire data set.

Depending on the complexity of your data transformations, incremental materializations may not always be a viable option to meet your transformation requirements. Incremental materializations are best suited for event or time-series data streamed directly into Rockset. To tell dbt which documents it should perform transformations on during an incremental run, provide SQL that filters for these documents using the ```is_incremental()``` macro in your dbt code. You can learn more about configuring incremental models in dbt [here](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models#how-do-i-use-the-incremental-materialization).

### Materializing dbt Table Models in Rockset

A [Table Model](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#table) is a transformation that drops and recreates an entire Rockset collection with the execution of ```dbt```. It updates that collection's transformed data with the most up-to-date source data. This is the simplest way to persist transformed data in Rockset, and results in much faster queries since the transformations are completed prior to query time.

However, Table Models can be slow to complete, since Rockset is not optimized for creating entirely new collections from scratch on the fly. This may significantly increase your data latency, as it may take several minutes for Rockset to provision resources for a new collection and then populate it with transformed data.

### Putting It All Together

You can use Table Models and Incremental Models (in conjunction with Rockset views), to customize the perfect stack to meet the unique requirements of your data transformations. For example, you can use SQL-based rollups to:

* Transform your streaming data during write-time.
* Transform and persist them into Rockset collections via Incremental or Table Models.
* Execute a sequence of view models during read-time to transform your data again.

## Testing, Formatting, & Caveats

### Testing Changes

Install [dbt-adapter-tests](https://github.com/dbt-labs/dbt-adapter-tests) in order to run the tests:
```
pip3 install pytest-dbt-adapter
```

Before landing a commit, ensure that your changes pass tests by inserting an api key for any active Rockset org in `test/rockset.dbtspec`, and then running these two commands to install your changes in your local environment and run our test suite:
```
pip3 install .
pytest test/rockset.dbtspec
```

### Formatting

Before landing a commit, format changes according to pep8 using these commands:
```
pip3 install autopep8
autopep8 --in-place --recursive .
```

### Caveats
1. `unique_key` is not supported with incremental, unless it is set to [_id](https://rockset.com/docs/special-fields/#the-_id-field), which acts as a natural `unique_key` in Rockset anyway.
2. The `table` materialization is slower in Rockset than most due to Rockset's architecture as a low-latency, real-time database. Creating new collections requires provisioning hot storage to index and serve fresh data, which takes about a minute.
3. Rockset queries have a two-minute timeout unless run asynchronously. You can extend this limit to 30 minutes by setting the run_async_iis to true. However, if the query ends up in the queue because you have hit your org's Concurrent Query Execution Limit (CQEL), the query must at least start execution before 2 minutes have passed. Otherwise, your IIS query will error. If the query leaves the queue and begins execution before 2 minutes have passed, the normal 30 minute time limit will apply.
