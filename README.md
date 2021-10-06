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
      database: N/A
      workspace: <rockset_workspace_name>
      api_key: <rockset_api_key>
  target: dev
```

Note that the `database` field is included in the profile, as this field is required by dbt-core. But Rockset does not have the typical concept of databases, so this value is not important.

## Supported Features

### Materializations

Type | Supported? | Details
-----|------------|----------------
table | YES | Creates a [Rockset collection](https://docs.rockset.com/collections/)
view | NO | Support coming soon
materializedview | NO | Support coming soon
ephemeral | NO | Support coming soon
incremental | NO | Support coming soon

### Formatting

Before landing a commit, format changes according to pep8 using these commands:
```
pip3 install autopep8
autopep8 --in-place --recursive .
```

