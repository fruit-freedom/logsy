import typing
import json

import aiohttp

class Task:
    """
    Supported log types:
    - JSON
    - Image file
    """
    id: int
    inputs: dict

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

        data = aiohttp.FormData()
        data.add_field('algorithm_name', algorithm_name)
        data.add_field('type', 'json')
        data.add_field('file', json.dumps(payload), filename='object.json', content_type='application/json')

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/objects', params=params, data=data) as response:
                print(await response.json())

    async def log_image(
        self,
        image_path: str,
        algorithm_name: str = None
    ):
        with open(image_path, 'rb') as file:
            image_data = file.read()

        params = { 'task_id': self.id } if self.id else { }
        data = aiohttp.FormData()
        data.add_field('algorithm_name', algorithm_name)
        data.add_field('type', 'image')
        data.add_field('file', image_data, filename=image_path, content_type='image')

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
