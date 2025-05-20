#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-chargebee',
      version='1.4.0',
      description='Singer.io tap for extracting data from the Chargebee API',
      author='dwallace@envoy.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_chargebee'],
      install_requires=[
          'singer-python==6.0.0',
          'backoff==2.2.1',
          'requests==2.31.0'
      ],
      entry_points='''
          [console_scripts]
          tap-chargebee=tap_chargebee:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_chargebee': [
              'schemas/common/*.json',
              'schemas/item_model/*.json',
              'schemas/plan_model/*.json'
          ]
      },
      include_package_data=True)

