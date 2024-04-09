#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-rockset"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.7.2"
description = """The Rockset adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Steven Baldwin",
    author_email="sbaldwin@rockset.com",
    url="https://github.com/rockset/dbt-rockset",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.7.0",
        "rockset_sqlalchemy~=0.0.1"
    ],
)
