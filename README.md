# dbt-rockset
[dbt](https://www.getdbt.com/) adapter for [Rockset](https://rockset.com/)

Note: this plugin is still under development, and is not yet suitable for production environments

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

