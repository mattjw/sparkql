from datetime import datetime

from sparkql import Struct, Float, String, Timestamp, Array


class Location(Struct):
    lat = Float(False, name="latitude")
    lon = Float(False, name="longitude")


class Conference(Struct):
    name = String(False)
    city = String(False)
    city_location = Location(False)
    country = String(False)
    twitter = String()
    start_date = Timestamp(False)
    end_date = Timestamp(False)
    url = String()
    keywords = Array(String(False), True)


prettified_schema = """
StructType(List(
    StructField(name,StringType,false),
    StructField(city,StringType,false),
    StructField(city_location,
        StructType(List(
            StructField(latitude,FloatType,false),
            StructField(longitude,FloatType,false))),
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


cardiff_pycon_dict = Conference.make_dict(
    name="PyCon UK",
    city="Cardiff",
    city_location=Location.make_dict(
        lat=51.48,
        lon=-3.18
    ),
    country="GBR",
    twitter=None,
    start_date=datetime(2019, 9, 13, 0, 0),
    end_date=datetime(2019, 9, 17, 13, 23, 59),
    url=None,
    keywords=["python", "software"]
)
