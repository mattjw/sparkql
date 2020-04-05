# sparkql ✨

[![PyPI version](https://badge.fury.io/py/sparkql.svg)](https://badge.fury.io/py/sparkql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/mattjw/sparkql/workflows/CI/badge.svg)](https://github.com/mattjw/sparkql/actions)
[![codecov](https://codecov.io/gh/mattjw/sparkql/branch/master/graph/badge.svg)](https://codecov.io/gh/mattjw/sparkql)

Python Spark SQL DataFrame schema management for sensible humans.

> _Don't sweat it... sparkql it ✨_

## Why use sparkql

`sparkql` takes the pain out of working with DataFrame schemas in PySpark.
It makes schema definition more Pythonic. And it's
particularly useful you're dealing with structured data.

In plain old PySpark, you might find that you write schemas
[like this](https://github.com/mattjw/sparkql/tree/master/examples/conferences_comparison/plain_schema.py):

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
[more literate](https://github.com/mattjw/sparkql/tree/master/examples/conferences_comparison/sparkql_schema.py):

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

Here's a summary of `sparkql`'s features.

- ORM-like class-based Spark schema definitions.
- Automated field naming: The attribute name of a field as it appears
  in its `Struct` is (by default) used as its field name. This name can
  be optionally overridden.
- Programatically reference field names in your structs with `path_col`
  and `path_str`. Avoid hand-constructing strings (or `Column`s) to
  reference your nested fields.
- Validate that a DataFrame matches a `sparkql` schema.
- Reuse and build composite schemas with `inheritance`, `includes`, and
  `implements`.
- Get a human-readable Spark schema representation with `pretty_schema`.
- Create an instance of a schema as a dictionary, with validation of
  the input values.

Read on for documentation on these features.

## Defining a schema

Each Spark atomic type has a counterpart `sparkql` field:

| PySpark type | `sparkql` field |
|---|---|
| `ByteType` | `Byte` |
| `IntegerType` | `Integer` |
| `LongType` | `Long` |
| `ShortType` | `Short` |
| `DecimalType` | `Decimal` |
| `DoubleType` | `Double` |
| `FloatType` | `Float` |
| `StringType` | `String` |
| `BinaryType` | `Binary` |
| `BooleanType` | `Boolean` |
| `DateType` | `Date` |
| `TimestampType` | `Timestamp` |

`Array` (counterpart to `ArrayType` in PySpark) allows the definition
of arrays of objects. By creating a subclass of `Struct`, we can
define a custom class that will be converted to a `StructType`.

For
[example](https://github.com/mattjw/sparkql/tree/master/examples/arrays/arrays.py),
given the `sparkql` schema definition:

```python
from sparkql import Struct, String, Array

class Article(Struct):
    title = String(nullable=False)
    tags = Array(String(), nullable=False)
    comments = Array(String(nullable=False))
```

Then we can build the equivalent PySpark schema (a `StructType`)
with:

```python
from sparkql import schema

pyspark_struct = schema(Article)
```

Pretty printing the schema with the expression
`sparkql.pretty_schema(pyspark_struct)` will give the following:

```text
StructType(List(
    StructField(title,StringType,false),
    StructField(tags,
        ArrayType(StringType,true),
        false),
    StructField(comments,
        ArrayType(StringType,false),
        true)))
```

## Features

Many examples of how to use `sparkql` can be found in
[`examples`](https://github.com/mattjw/sparkql/tree/master/examples).

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
    latitude = Float(name="lat")
    longitude = Float(name="lon")
```

which would mean the concrete name of the `Geolocation.latitude` field
is `lat`.

### Field paths and nested objects

Referencing fields in nested data can be a chore. `sparkql` simplifies this
with path referencing.

[For example](https://github.com/mattjw/sparkql/tree/master/examples/nested_objects/sparkql_example.py), if we have a
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
IDE auto-completion, automated refactoring, and identifying errors
before runtime.

`path_col` is the counterpart to `path_str` and returns a Spark `Column`
object for the path, allowing it to be used in all places where Spark
requires a column.

### DataFrame validation

Struct method `validate_data_frame` will verify if a given DataFrame's
schema matches the Struct.
[For example](https://github.com/mattjw/sparkql/tree/master/examples/validation/test_validation.py),
if we have our `Article`
struct and a DataFrame we want to ensure adheres to the `Article`
schema:

```python
dframe = spark_session.createDataFrame([{"title": "abc"}])

class Article(Struct):
    title = String()
    body = String()
```

Then we can can validate with:

```python
validation_result = Article.validate_data_frame(dframe)
```

`validation_result.is_valid` indicates whether the DataFrame is valid
(`False` in this case), and `validation_result.report` is a
human-readable string describing the differences:

```text
Struct schema...

StructType(List(
    StructField(title,StringType,true),
    StructField(body,StringType,true)))

DataFrame schema...

StructType(List(
    StructField(title,StringType,true)))

Diff of struct -> data frame...

  StructType(List(
-     StructField(title,StringType,true)))
+     StructField(title,StringType,true),
+     StructField(body,StringType,true)))
```

For convenience,

```python
Article.validate_data_frame(dframe).raise_on_invalid()
```

will raise a `InvalidDataFrameError` (see `sparkql.exceptions`) if the  
DataFrame is not valid.

### Creating an instance of a schema

`sparkql` simplifies the process of creating an instance of a struct.
You might need to do this, for example, when creating test data, or
when creating an object (a dict or a row) to return from a UDF.

Use `Struct.make_dict(...)` to instantiate a struct as a dictionary.
This has the advantage that the input values will be correctly
validated, and it will convert schema property names into their
underlying field names.

For
[example](https://github.com/mattjw/sparkql/tree/master/examples/struct_instantiation/instantiate_as_dict.py),
given some simple Structs:

```python
class User(Struct):
    id = Integer(name="user_id", nullable=False)
    username = String()

class Article(Struct):
    id = Integer(name="article_id", nullable=False)
    title = String()
    author = User()
    text = String(name="body")
```

Here are a few examples of creating dicts from `Article`:

```python
Article.make_dict(
    id=1001,
    title="The article title",
    author=User.make_dict(
        id=440,
        username="user"
    ),
    text="Lorem ipsum article text lorem ipsum."
)

# generates...
{
    "article_id": 1001,
    "author": {
        "user_id": 440,
        "username": "user"},
    "body": "Lorem ipsum article text lorem ipsum.",
    "title": "The article title"
}
```

```python
Article.make_dict(
    id=1002
)

# generates...
{
    "article_id": 1002,
    "author": None,
    "body": None,
    "title": None
}
```

See
[this example](https://github.com/mattjw/sparkql/tree/master/examples/conferences_extended/conferences.py)
for an extended example of using `make_dict`.

### Composite schemas

It is sometimes useful to be able to re-use the fields of one struct
in another struct. `sparkql` provides a few features to enable this:

- _inheritance_: A subclass inherits the fields of a base struct class.
- _includes_: Incorporate fields from another struct.
- _implements_: Enforce that a struct must implement the fields of
  another struct.

See the following examples for a better explanation.

#### Using inheritance

For [example](https://github.com/mattjw/sparkql/tree/master/examples/composite_schemas/inheritance.py), the following:

```python
class BaseEvent(Struct):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)

class RegistrationEvent(BaseEvent):
    user_id = String(nullable=False)
```

will produce the following `RegistrationEvent` schema:

```text
StructType(List(
    StructField(correlation_id,StringType,false),
    StructField(event_time,TimestampType,false),
    StructField(user_id,StringType,false)))
```

#### Using an `includes` declaration

For [example](https://github.com/mattjw/sparkql/tree/master/examples/composite_schemas/includes.py), the following:

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

#### Using an `implements` declaration

`implements` is similar to `includes`, but does not automatically
incorporate the fields of specified structs. Instead, it is up to
the implementor to ensure that the required fields are declared in
the struct.

Failing to implement a field from an `implements` struct will result in
a `StructImplementationError` error.

[For example](https://github.com/mattjw/sparkql/tree/master/examples/composite_schemas/implements.py):

```
class LogEntryMetadata(Struct):
    logged_at = Timestamp(nullable=False)

class PageViewLogEntry(Struct):
    class Meta:
        implements = [LogEntryMetadata]
    page_id = String(nullable=False)

# the above class declaration will fail with the following StructImplementationError error:
#   Struct 'RegistrationEvent' does not implement field 'correlation_id' required by struct 'EventMetadata'
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
