from sparkql import Struct, Timestamp, String


#
# Simple example of re-using common fields via `includes`

class EventMetadata(Struct):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)


class PurchaseEvent(Struct):
    class Meta:
        includes = [EventMetadata]
    item_id = String(nullable=False)


class RegistrationEvent(Struct):
    class Meta:
        includes = [EventMetadata]
    user_id = String(nullable=False)


#
# Here's what the schemas look like

prettified_purchase_event_schema = """
StructType(List(
    StructField(item_id,StringType,false),
    StructField(correlation_id,StringType,false),
    StructField(event_time,TimestampType,false)))
"""

prettified_registration_event_schema = """
StructType(List(
    StructField(user_id,StringType,false),
    StructField(correlation_id,StringType,false),
    StructField(event_time,TimestampType,false)))
"""
