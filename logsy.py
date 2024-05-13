import typing
import json
import io
import typing

import aiohttp
from PIL import Image


class Group:
    id: int
    task_id: int
    name: str

    def __init__(self, id: int, task_id: int, name: str) -> None:
        self.id = id
        self.task_id = task_id
        self.name = name

    @staticmethod
    async def init(task_id: int, name: str):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/groups', json={ 'task_id': task_id, 'name': name }) as response:
                body = await response.json()
                print('Created group', body)
                return Group(body['id'], body['task_id'], body['name'])


class Object:
    id: int
    type: str
    path: str
    path_type: str
    algorithm_name: str
    meta: dict

    def __init__(self, id: int, type: str, path: str, path_type: str, algorithm_name: str, meta: dict) -> None:
        self.id = id
        self.type = type
        self.path = path
        self.path_type = path_type
        self.algorithm_name = algorithm_name
        self.meta = meta

    @staticmethod
    async def get(task_id: int):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(f'http://localhost:8000/api/tasks/{task_id}') as response:
                body = await response.json()
                return Object(
                    body['id'],
                    body['type'],
                    body['path'],
                    body['path_type'],
                    body['algorithm_name'],
                    body['meta']
                )


class Task:
    """
    Supported log types:
    - JSON
    - Image file

    Can be astraction of s3 storage or gRPC yield_channel

    """
    id: int
    inputs: dict

    def __init__(self, id: int = None, inputs: dict = None) -> None:
        self.id = id
        self.inputs = inputs if inputs else { }

    async def create_group(self, name: str):
        return await Group.init(self.id, name)


    async def log_json(
        self,
        object: dict | list = None,
        path: str = None,
        file_content: str = None,
        file: str = None,
        algorithm_name: str = None,
        meta: dict = {},
        upload: bool = True
    ):
        params = { 'task_id': self.id } if self.id else { }
        data = aiohttp.FormData()
        data.add_field('algorithm_name', algorithm_name)
        data.add_field('type', 'json')
        data.add_field('meta', json.dumps(meta))

        if object:
            data.add_field('file', json.dumps(object), filename='file.json')
        elif path:
            if upload:
                data.add_field('file', open(path, 'rb'), filename='file.json')
            else:
                data.add_field('path', path)
        elif file_content:
            data.add_field('file', file_content, filename='file.json')
        elif file:
            data.add_field('file', file, filename='file.json')

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/objects', params=params, data=data) as response:
                print(await response.json())


    async def log_image(
        self,
        path: str = None,
        file_content: str = None,
        file: typing.BinaryIO = None,
        algorithm_name: str = None,
        meta: dict = {},
        upload: bool = True
    ):
        params = { 'task_id': self.id } if self.id else { }
        data = aiohttp.FormData()
        data.add_field('algorithm_name', algorithm_name)
        data.add_field('type', 'image')
        data.add_field('meta', json.dumps(meta))

        if path:
            if upload:
                extension = Image.open('image.jpg').format.lower()
                data.add_field('file', open(path, 'rb'), filename=f'file.{extension}')
            else:
                data.add_field('path', path)
        elif file_content:
            extension = Image.open(io.BytesIO(file_content)).format.lower()
            data.add_field('file', file_content, filename=f'file.{extension}')
        elif file:
            extension = Image.open(file).format.lower()
            file.seek(0)
            data.add_field('file', file, filename=f'file.{extension}')

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/objects', params=params, data=data) as response:
                print(await response.json())

    # async def log_xyz(
    #     self,
    #     path: str,
    #     algorithm_name: str = None,
    #     meta: dict = None
    # ):
    #     params = { 'task_id': self.id } if self.id else { }
    #     data = aiohttp.FormData()
    #     data.add_field('algorithm_name', algorithm_name)
    #     data.add_field('type', 'xyz')
    #     data.add_field('path', path)
    #     if meta:
    #         data.add_field('meta', json.dumps(meta))

    #     async with aiohttp.ClientSession(raise_for_status=False) as session:
    #         async with session.post('http://localhost:8000/api/objects', params=params, data=data) as response:
    #             print(await response.json())


    async def log_geotiff(
        self,
        path: str = None,
        file_content: str = None,
        file: str = None,
        algorithm_name: str = None,
        meta: dict = {},
        upload: bool = True
    ):
        params = { 'task_id': self.id } if self.id else { }
        data = aiohttp.FormData()
        data.add_field('algorithm_name', algorithm_name)
        data.add_field('type', 'geotiff')
        data.add_field('meta', json.dumps(meta))

        if path:
            if upload:
                data.add_field('file', open(path, 'rb'), filename='file.tiff')
            else:
                data.add_field('path', path)
        elif file_content:
            data.add_field('file', file_content, filename='file.tiff')
        elif file:
            data.add_field('file', file, filename='file.tiff')

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

    @staticmethod
    async def get(task_id: int):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(f'http://localhost:8000/api/tasks/{task_id}') as response:
                body = await response.json()
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


class Object:
    @staticmethod
    async def init():
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post('http://localhost:8000/api/objects') as response:
                body = await response.json()
