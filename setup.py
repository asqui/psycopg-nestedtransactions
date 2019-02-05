#!/usr/bin/env python

from distutils.core import setup

setup(name='psycopg-nestedtransactions',
      version='1.0',
      description='Database transaction manager for psycopg2 database connections with seamless support for nested transactions.',
      url='https://github.com/asqui/psycopg-nestedtransactions',
      packages=['nestedtransactions'],
      install_requires=['psycopg2', 'pg8000<=1.12.4;python_version<"3"'],
      extras_require=dict(
          test=['pytest', 'testing.postgresql']
      )
      )
