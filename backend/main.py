from contextlib import aclosing

from fastapi import Depends, FastAPI
from fastapi.sse import EventSourceResponse, ServerSentEvent

from subscriber import consume_events

app = FastAPI()

async def event_source():
    async with aclosing(consume_events()) as stream:
        yield stream

@app.get("/events", response_class=EventSourceResponse)
async def stream_events(stream=Depends(event_source)):
    async for message_id, event in stream:
        yield ServerSentEvent(
            id=message_id,
            data=event,
        )

        