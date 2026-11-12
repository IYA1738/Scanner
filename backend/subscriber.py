import json
from redis.asyncio import Redis

async def consume_events():
    last_id = "0-0"

    async with Redis(host="127.0.0.1", decode_responses=True) as client:
        while True:
            streams = await client.xread(
                {"scanner:events": last_id},
                count=100,
                block=1000,
            )

            for _, messages in streams:
                for message_id, fields in messages:
                    event = json.loads(fields["data"])
                    yield message_id, event
                    last_id = message_id
