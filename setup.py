#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-rockset"
package_version = "0.1.5"
description = """The Rockset adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Venkat Venkataramani',
    author_email='hello@rockset.com',
    url='https://github.com/rockset/dbt-rockset',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/rockset/macros/*.sql',
            'include/rockset/macros/**/*.sql',
            'include/rockset/dbt_project.yml',
        ]
    },
    install_requires=[
        "dbt-core>=0.18",
        "rockset==0.8.10"
    ]
)
