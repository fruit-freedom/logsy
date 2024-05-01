import typing
import json

import aiohttp

import log_types

class Task:
    """
    Supported log types:
    - JSON
    - Image file
    """
    id: int

    def __init__(self, id: int = None, inputs: dict = None) -> None:
        self.id = id
        self.inputs = inputs if inputs else { }

    async def log_json(
        self,
        payload: typing.Any,
        algorithm_name: str = None
    ):
        """
        Interface
        ---------
        
        Can be astraction of s3 storage or gRPC yield_channel
        """        
        params = { 'task_id': self.id } if self.id else { }
        data = {
            'data': json.dumps(payload),
            'algorithm_name': algorithm_name
        }
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/objects', params=params, data=data) as response:
                print(await response.json())

    async def log(self, obj):
        if isinstance(obj, log_types.GeoTiffPath):
            pass
        elif isinstance(obj, log_types.JSON):
            pass

    async def log_image(
        self,
        image_path: str,
        algorithm_name: str = None
    ):
        params = { 'task_id': self.id } if self.id else { }
        data = {
            'image_path': image_path,
            'algorithm_name': algorithm_name
        }
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/objects', params=params, data=data) as response:
                print(await response.json())

    @staticmethod
    async def init(inputs: dict = {}):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/tasks', json={ 'inputs': inputs }) as response:
                body = await response.json()
                print('Created task', body)
                return Task(id=body['id'])

    async def set_result(self):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.patch(f'http://localhost:8000/api/tasks/{self.id}', json={ 'status': 'completed' }) as response:
                pass

    async def set_exception(self, stacktrace=None):
        json={ 'status': 'aborted', 'stacktrace': stacktrace }
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.patch(f'http://localhost:8000/api/tasks/{self.id}', json=json) as response:
                pass

    async def set_progress(self, progress: float): pass
