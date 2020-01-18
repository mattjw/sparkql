# sparkql ✨

[![PyPI version](https://badge.fury.io/py/sparkql.svg)](https://badge.fury.io/py/sparkql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CircleCI](https://circleci.com/gh/mattjw/sparkql.svg?style=svg)](https://circleci.com/gh/mattjw/sparkql)

Python Spark SQL DataFrame schema management for sensible humans.

## Why use sparkql

sparkql takes the pain out of working with DataFrame schemas in PySpark. It's
particularly useful when you have structured data.

You might find that you write schemas like this:

```python
CITY_SCHEMA = StructType()
CITY_NAME_FIELD = "name"
CITY_SCHEMA.add(StructField(CITY_NAME_FIELD, StringType(), False))
CITY_LAT_FIELD = "latitude"
CITY_SCHEMA.add(StructField(CITY_LAT_FIELD, FloatType()))
CITY_LONG_FIELD = "longitude"
CITY_SCHEMA.add(StructField(CITY_LONG_FIELD, FloatType()))

CONFERENCE_SCHEMA = StructType()
CONF_NAME_FIELD = "name"
CONFERENCE_SCHEMA.add(StructField(CONF_NAME_FIELD, StringType(), False))
CONF_CITY_FIELD = "city"
CONFERENCE_SCHEMA.add(StructField(CONF_CITY_FIELD, CITY_SCHEMA))
```

And use it like this:

```python
df.withColumn("city_name", df[CONF_CITY_FIELD][CITY_NAME_FIELD])
```

With sparkql, schemas become more pythonic and literate:

```python

```

## Contributing

Developers who'd like to contribute to this project should refer to
[CONTRIBUTING.md](./CONTRIBUTING.md).
