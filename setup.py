#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-typeform",
    version="2.4.1",
    description="Singer.io tap for extracting data from the TypeForm Responses API",
    author="bytcode.io",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python==6.0.0",
        "pendulum==3.0.0",
        "ratelimit==2.2.1",
        "backoff==2.2.1",
        "requests==2.32.3",
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
            'nose2',
            "parameterized",
        ]
    },
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
