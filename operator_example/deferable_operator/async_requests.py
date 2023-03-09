import asyncio
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.models.baseoperator import BaseOperator
from typing import Any
from datetime import datetime
from airflow import DAG
import requests
# import aiohttp


class LoopRequests(BaseTrigger):

    def __init__(self):
        super().__init__()

    def serialize(self):
        return ("async_requests.LoopRequests", {})

    async def run(self):
        loop = asyncio.get_event_loop()

        try:
            await loop.create_task(requests.get('https://google.po', timeout=5))
            # async with aiohttp.ClientSession() as session:
            #     async with session.get('https://google.po') as response:
            #         await response.json()
        except:
            yield TriggerEvent({"status": "404"})


class HTTPStatusOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context) -> Any:
        self.defer(trigger=LoopRequests(), method_name='execute_complete')

    def execute_complete(self, context, event):
        # event is the payload send by the trigger when it yields the TriggerEvent
        print(event['status'])

        return event

with DAG(dag_id="loop_requests", schedule_interval='@daily', start_date=datetime(2022, 11, 15), catchup=False) as dag:

    get_http_status = HTTPStatusOperator(
        task_id='get_http_status'
    )

    get_http_status
