import asyncio
import logging

from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage

logging.getLogger().setLevel(logging.INFO)

async def main() -> None:
    # Perform connection
    connection = await connect("amqp://admin:admin@localhost/")

    # Creating a channel
    channel = await connection.channel()
    exchange = await channel.get_exchange('logsy-events')

    # Declaring queue
    queue = await channel.declare_queue(durable=False, auto_delete=True)
    await queue.bind(exchange)

    logging.info('Waiting for events')
    async with queue.iterator() as qiterator:
        message: AbstractIncomingMessage
        async for message in qiterator:
            try:
                async with message.process(requeue=False):
                    logging.info('Message:', message.body)
            except Exception:
                logging.exception('Processing error for message %r', message)

if __name__ == "__main__":
    asyncio.run(main())
