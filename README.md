# sparkql ✨

[![PyPI version](https://badge.fury.io/py/sparkql.svg)](https://badge.fury.io/py/sparkql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CircleCI](https://circleci.com/gh/mattjw/sparkql.svg?style=svg)](https://circleci.com/gh/mattjw/sparkql)

Python Spark SQL DataFrame schema management for sensible humans.

## Why use `sparkql`

`sparkql` takes the pain out of working with DataFrame schemas in PySpark.
In general, it makes schema definition more Pythonic, and it's
particularly useful you're dealing with structured data.

In plain old PySpark, you might find that you write schemas
[like this](./examples/conferences_comparison/plain_schema.py):

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

And then plain old PySpark makes you deal with nested fields like this:

```python
dframe.withColumn("city_name", df[CONF_CITY_FIELD][CITY_NAME_FIELD])
```

Instead, with `sparkql`, schemas become a lot
[more literate](./examples/conferences_comparison/sparkql_schema.py):

```python
class City(Struct):
    name = String(nullable=False)
    latitude = Float()
    longitude = Float()

class Conference(Struct):
    name = String(nullable=False)
    city = City()
```

As does dealing with nested fields:

```python
dframe.withColumn("city_name", path_col(Conference.city.name))
```

## Features

### Automated field naming

By default, field names are inferred from the attribute name in the
struct they are declared.

For example, given the struct

```python
class Geolocation(Struct):
    latitude = Float()
    longitude = Float()
```

the concrete name of the `Geolocation.latitude` field is `latitude`.

Names also be overridden by explicitly specifying the field name as an
argument to the field

```python
class Geolocation(Struct):
    latitude = Float("lat")
    longitude = Float("lon")
```

which would mean the concrete name of the `Geolocation.latitude` field
is `lat`.

### Field paths, and nested objects

Referencing fields in nested data can be a chore. `sparkql` simplifies this
with path referencing.

[For example](./examples/nested_objects/sparkql_example.py), if we have a
schema with nested objects:

```python
class Address(Struct):
    post_code = String()
    city = String()


class User(Struct):
    username = String(nullable=False)
    address = Address()


class Comment(Struct):
    message = String()
    author = User(nullable=False)


class Article(Struct):
    title = String(nullable=False)
    author = User(nullable=False)
    comments = Array(Comment())
```

We can use `path_str` to turn a path into a Spark-understandable string:

```python
author_city_str = path_str(Article.author.address.city)
"author.address.city"
```

For paths that include an array, two approaches are provided:

```python
comment_usernames_str = path_str(Article.comments.e.author.username)
"comments.author.username"

comment_usernames_str = path_str(Article.comments.author.username)
"comments.author.username"
```

Both give the same result. However, the former (`e`) is more
type-oriented. The `e` attribute corresponds to the array's element
field. Although this looks strange at first, it has the advantage of
being inspectable by IDEs and other tools, allowing goodness such as
IDE auto-completion and IDE-assisted refactoring.

### Composite schemas

Structs can be re-used to build composite schemas with _inheritance_ or _includes_.

#### Using inheritance

For [example](./examples/composite_schemas/inheritance.py), the following:

```python
class BaseEvent(Struct):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)

class RegistrationEvent(BaseEvent):
    user_id = String(nullable=False)
```

will produce the `RegistrationEvent` schema:

```text
StructType(List(
    StructField(correlation_id,StringType,false),
    StructField(event_time,TimestampType,false),
    StructField(user_id,StringType,false)))
```

#### Using an `includes` declaration

For [example](./examples/composite_schemas/includes.py), the following:

```python
class EventMetadata(Struct):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)

class RegistrationEvent(Struct):
    class Meta:
        includes = [EventMetadata]
    user_id = String(nullable=False)
```

will produce the `RegistrationEvent` schema:

```text
StructType(List(
    StructField(user_id,StringType,false),
    StructField(correlation_id,StringType,false),
    StructField(event_time,TimestampType,false)))
```

### Prettified Spark schema strings

Spark's stringified schema representation isn't very user friendly, particularly for large schemas:


```text
StructType(List(StructField(name,StringType,false),StructField(city,StructType(List(StructField(name,StringType,false),StructField(latitude,FloatType,true),StructField(longitude,FloatType,true))),true)))
```

The function `pretty_schema` will return something more useful:

```text
StructType(List(
    StructField(name,StringType,false),
    StructField(city,
        StructType(List(
            StructField(name,StringType,false),
            StructField(latitude,FloatType,true),
            StructField(longitude,FloatType,true))),
        true)))
```

## Contributing

Contributions are very welcome. Developers who'd like to contribute to
this project should refer to [CONTRIBUTING.md](./CONTRIBUTING.md).
