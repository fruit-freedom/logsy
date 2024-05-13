import enum

import pydantic
import datetime

class EventType(enum.Enum):
    TaskCreated = 'task:created'
    TaskUpdated = 'task:updated'
    ObjectCreated = 'object:created'
    ObjectUpdated = 'object:updated'


class Event(pydantic.BaseModel):
    time: datetime.datetime
    type: EventType
    instance: dict
