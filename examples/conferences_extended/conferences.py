from sparkql import StructObject, Float, String, Timestamp, Array


class Location(StructObject):
    lat = Float(False)
    lon = Float(False)


class Conference(StructObject):
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
