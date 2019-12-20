from sparkql import StructObject, FloatField, StringField, TimestampField, ArrayField


class Location(StructObject):
    lat = FloatField(False)
    lon = FloatField(False)


class Conference(StructObject):
    name = StringField(False)
    city = StringField(False)
    city_location = Location(False)
    country = StringField(False)
    twitter = StringField()
    start_date = TimestampField(False)
    end_date = TimestampField(False)
    url = StringField()
    keywords = ArrayField(StringField(False), True)


prettified_schema = """
StructType(List(
    StructField(name,StringType,false),
    StructField(city,StringType,false),
    StructField(city_location,
        StructType(List(
            StructField(lat,FloatType,false),
            StructField(lon,FloatType,false))),
        false),
    StructField(country,StringType,false),
    StructField(twitter,StringType,true),
    StructField(start_date,TimestampType,false),
    StructField(end_date,TimestampType,false),
    StructField(url,StringType,true),
    StructField(keywords,
        ArrayType(StringType,false),
        true)))
"""

