from sparkql import Struct, Timestamp, String


#
# Simple example of re-using common fields via inheritance

class BaseEvent(Struct):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)


class PurchaseEvent(BaseEvent):
    item_id = String(nullable=False)


class RegistrationEvent(BaseEvent):
    user_id = String(nullable=False)


#
# Here's what the schemas look like

prettified_purchase_event_schema = """
StructType(List(
    StructField(correlation_id,StringType,false),
    StructField(event_time,TimestampType,false),
    StructField(item_id,StringType,false)))
"""

prettified_registration_event_schema = """
StructType(List(
    StructField(correlation_id,StringType,false),
    StructField(event_time,TimestampType,false),
    StructField(user_id,StringType,false)))
"""
