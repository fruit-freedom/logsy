import asyncio
import logging
import datetime

from aio_pika import Message, connect
import aio_pika

from . import models
import settings

logging.getLogger().setLevel(logging.INFO)

class AbstractEventsQueue:
    def __init__(self) -> None: pass
    async def init(self): pass
    async def produce_event(self, type: models.EventType, instance: dict): pass
    async def close(self): pass

class MockEventsQueue(AbstractEventsQueue):
    pass

class RabbitMQEventsQueue(AbstractEventsQueue):
    async def init(self):
        self.connection = await connect(settings.RQ_CONNECTION_STRING)

        # Creating a channel
        channel = await self.connection.channel()
        self.exchange = await channel.declare_exchange(settings.RQ_EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT)

    async def produce_event(self, type: models.EventType, instance: dict):
        event_model = models.Event.model_validate({
            'time': datetime.datetime.now(),
            'type': type,
            'instance': instance
        })
        await self.exchange.publish(
            Message(body=event_model.model_dump_json().encode()),
            "logsy-event"
        )

    async def close(self):
        await self.connection.close()

events_queue = RabbitMQEventsQueue() if settings.USE_RABBITMQ_EVENTS else MockEventsQueue()

async def main():
    await events_queue.init()
    await events_queue.produce_event(models.EventType.TaskCreated, { 'id': 1, 'name': 'None' })

if __name__ == "__main__":
    asyncio.run(main())
