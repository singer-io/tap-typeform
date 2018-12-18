#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-typeform",
    version="1.0.1",
    description="Singer.io tap for extracting data from the TypeForm Responses API",
    author="bytcode.io",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python==5.4.0",
        "pendulum",
        "ratelimit",
        "backoff==1.3.2",
        "requests==2.20.0",
    ],
    entry_points="""
    [console_scripts]
    tap-typeform=tap_typeform:main
    """,
    packages=find_packages(),
    package_data = {
        "schemas": ["tap_typeform/schemas/*.json"]
    },
    include_package_data=True
)
