from contextlib import asynccontextmanager
import json
import uuid
import os
import enum
from typing import Annotated
import datetime

import fastapi
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey, JSON
import sqlalchemy
from sqlalchemy.dialects.postgresql import ENUM, JSON
import pydantic
import aiohttp
import grpc

import tiler_pb2_grpc
import tiler_pb2
import settings

class Base(AsyncAttrs, DeclarativeBase):
    pass


class TaskStatusEnum(enum.Enum):
    created     = 'created'
    started     = 'started'
    running     = 'running'
    completed   = 'completed'
    aborted     = 'aborted'


class SourceCode(Base):
    __tablename__ = "source_code"

    id: Mapped[int] = mapped_column(primary_key=True)
    entry_point: Mapped[str] = mapped_column()


class Task(Base):
    __tablename__ = "task"

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(ENUM(*(e.value for e in TaskStatusEnum), name='task_status_enum'))
    stacktrace: Mapped[str] = mapped_column(nullable=True)
    source_code_id: Mapped[int] = mapped_column(ForeignKey("source_code.id"), nullable=True)
    inputs: Mapped[str] = mapped_column(JSON(), nullable=True)
    start_time: Mapped[datetime.datetime] = mapped_column(sqlalchemy.DateTime(True))


class PathTypeEnum(enum.Enum):
    relative = 'relative'
    absolute = 'absolute'
    url = 'url'


class Object(Base):
    __tablename__ = "object"

    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column()
    path_type: Mapped[str] = mapped_column(sqlalchemy.Enum(PathTypeEnum), default=PathTypeEnum.absolute, nullable=False)
    task_id: Mapped[int] = mapped_column(ForeignKey("task.id"))
    algorithm_name: Mapped[str] = mapped_column(nullable=True)
    type: Mapped[str] = mapped_column(sqlalchemy.String(64))
    meta: Mapped[str] = mapped_column(JSON(none_as_null=True))
    preview_path: Mapped[str] = mapped_column


class Group(Base):
    __tablename__ = "group"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[int] = mapped_column(ForeignKey("task.id"))
    name: Mapped[str] = mapped_column(nullable=True)

# class ObjectGroup(Base):
#     __tablename__ = "object_group"

#     object_id: Mapped[int] = mapped_column(ForeignKey("object.id"))
#     group_id: Mapped[int] = mapped_column(ForeignKey("group.id"))


@asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    # async with engine.begin() as conn:
    #     await conn.run_sync(Base.metadata.drop_all)
    #     await conn.run_sync(Base.metadata.create_all)
    yield


app = fastapi.FastAPI(lifespan=lifespan)
engine = create_async_engine(
    "postgresql+asyncpg://postgres:postgres@localhost/logsy",
    echo=True,
)
async_session = async_sessionmaker(engine, expire_on_commit=False)
if not os.path.exists(settings.STORAGE_DIRECTORY):
    os.makedirs(settings.STORAGE_DIRECTORY)


@app.get('/api/storage/{filepath:path}')
async def get_file(filepath: str):
    return fastapi.responses.FileResponse(os.path.join(settings.STORAGE_DIRECTORY, filepath))


class CreateTaskTemplateRequest(pydantic.BaseModel):
    source_code: str


@app.post('/api/source-code')
async def create_task_template(body: CreateTaskTemplateRequest = fastapi.Body()):
    entry_point = f'src_{ str(uuid.uuid4()).replace("-", "")[:8] }.py'
    path = os.path.join('storage', entry_point)

    with open(path, 'w') as file:
        file.write(body.source_code)

    async with async_session.begin() as session:
        source_code = SourceCode(entry_point=entry_point)
        session.add(source_code)
    return source_code


class CreateTaskRequest(pydantic.BaseModel):
    source_code_id: int = None
    inputs: dict = None


@app.post('/api/tasks')
async def create_task(body: CreateTaskRequest):
    async with async_session.begin() as session:
        task = Task(
            status=TaskStatusEnum.created.value,
            source_code_id=body.source_code_id,
            inputs=body.inputs,
            start_time=datetime.datetime.now()
        )
        session.add(task)
    return task


@app.post('/api/agent/tasks/{task_id}/run')
async def run_task(task_id: int):
    async with async_session.begin() as session:
        task = await session.get_one(Task, task_id)
        if not task.source_code_id:
            raise fastapi.HTTPException(status_code=400)
        source_code = await session.get_one(SourceCode, task.source_code_id)

    json = {
        'task_id': task.id,
        'entry_point': source_code.entry_point
    }
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with session.post('http://localhost:8001/tasks/run', json=json) as response:
            pass


@app.get('/api/tasks')
async def get_tasks_list():
    async with async_session.begin() as session:
        stmt = sqlalchemy.select(Task)
        result = await session.execute(stmt)
        return result.scalars().all()


@app.get('/api/tasks/{task_id}')
async def get_task(task_id: int):
    async with async_session.begin() as session:
        instance = await session.get(Task, task_id)
        if not instance:
            raise fastapi.HTTPException(status_code=404)
        return instance


class UpdateTaskRequest(pydantic.BaseModel):
    status: TaskStatusEnum
    stacktrace: str = None


@app.patch('/api/tasks/{task_id}')
async def update_task(task_id: int, body: UpdateTaskRequest = fastapi.Body()):
    async with async_session.begin() as session:
        stmt = sqlalchemy.update(Task)
        stmt = stmt.values(status=body.status.value) if body.status else stmt
        stmt = stmt.values(stacktrace=body.stacktrace) if body.stacktrace else stmt
        stmt = stmt.where(Task.id == task_id)
        await session.execute(stmt)


class ObjectTypeEnum(enum.Enum):
    Image = 'image'
    JSON = 'json'
    XYZ = 'xyz'
    GeoTiff = 'geotiff'


@app.post('/api/objects')
async def create_object(
    task_id: int,
    type: Annotated[ObjectTypeEnum, fastapi.Form()],
    file: fastapi.UploadFile = None,
    path: Annotated[str, fastapi.Form()] = None,
    algorithm_name: Annotated[str, fastapi.Form()] = None,
    meta: Annotated[str, fastapi.Form()] = None,
):
    meta = json.loads(meta) if meta else {}

    if file:
        path_type = PathTypeEnum.absolute
        _, file_extension = os.path.splitext(file.filename)
        path = f'{uuid.uuid4()}{file_extension}'

        with open(os.path.join(settings.STORAGE_DIRECTORY, path), 'wb') as outfile:
            outfile.write(await file.read())
    elif path:
        path_type = PathTypeEnum.relative
    else:
        raise fastapi.HTTPException(422, detail="Path or file should be specified")

    if type == ObjectTypeEnum.GeoTiff:
        async with grpc.aio.insecure_channel("localhost:50051") as channel:
            stub = tiler_pb2_grpc.TilerServiceStub(channel)
            response: tiler_pb2.CreateTilesResponse = await stub.CreateTiles(tiler_pb2.CreateTilesRequest(path=path))
            if response.error:
                print(response.error)
                raise fastapi.HTTPException(400, detail="can not tile geotiff")
            print("Received: ", response)
            meta = json.loads(response.meta)
            meta['xyz'] = f'{response.path}/{{z}}/{{x}}/{{-y}}.{meta["extension"]}'

    async with async_session.begin() as session:
        object = Object(
            task_id=task_id,
            path=path,
            type=type.value,
            algorithm_name=algorithm_name,
            meta=meta,
            path_type=path_type
        )
        session.add(object)
    return object


@app.get('/api/objects')
async def get_objects_list(task_id: int = None):
    async with async_session.begin() as session:
        stmt = sqlalchemy.select(Object)
        if task_id:
            stmt = stmt.where(Object.task_id == task_id)
        result = await session.execute(stmt)
        return result.scalars().all()


@app.get('/api/objects/{object_id}')
async def get_object(object_id: int):
    async with async_session.begin() as session:
        instance = await session.get(Object, object_id)
        if not instance:
            raise fastapi.HTTPException(status_code=404)
        return instance


@app.get('/api/objects/{object_id}/data')
async def get_object(object_id: int):
    async with async_session.begin() as session:
        instance = await session.get(Object, object_id)
        if not instance:
            raise fastapi.HTTPException(status_code=404)
        return fastapi.responses.FileResponse(instance.path)


class CreateGroupRequest(pydantic.BaseModel):
    task_id: int
    name: str


@app.post('/api/groups')
async def create_group(request: CreateGroupRequest):

    async with async_session.begin() as session:
        group = Group(
            task_id=request.task_id,
            name=request.name
        )
        session.add(group)
    return group


@app.get('/api/groups')
async def get_groups_list(task_id: int = None):
    async with async_session.begin() as session:
        stmt = sqlalchemy.select(Group)
        if task_id:
            stmt = stmt.where(Group.task_id == task_id)
        result = await session.execute(stmt)
        return result.scalars().all()


@app.get('/api/groups/{group_id}')
async def get_object(group_id: int):
    async with async_session.begin() as session:
        instance = await session.get(Group, group_id)
        if not instance:
            raise fastapi.HTTPException(status_code=404)
        return instance
    
