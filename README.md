# dbt-rockset
[dbt](https://www.getdbt.com/) adapter for [Rockset](https://rockset.com/)

## Installation
This plugin can be installed with:
```
pip install dbt-rockset
```

## Configuring your profile
An example Rockset dbt profile is shown below:

```
rockset:
  outputs:
    dev:
      type: rockset
      workspace: <rockset_workspace_name>
      api_key: <rockset_api_key>
      api_server: <rockset_api_server> # Default is `api.rs2.usw2.rockset.com`, which is the api_server endpoint for region us-west-2.
  target: dev
```

## Supported Features

### Materializations

Type | Supported? | Details
-----|------------|----------------
table | YES | Creates a [Rockset collection](https://docs.rockset.com/collections/).
view | YES | Creates a [Rockset view](https://rockset.com/docs/views/#gatsby-focus-wrapper).
ephemeral | Yes | Create a CTE.
incremental | YES | Creates a [Rockset collection](https://docs.rockset.com/collections/) if it doesn't exist, and writes to it.

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
3. Rockset queries have a two-minute timeout. Any model which runs a query that takes longer to execute than two minutes will fail.

