from contextlib import asynccontextmanager
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
from sqlalchemy.dialects.postgresql import ENUM
import pydantic
import aiohttp


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


class ObjectTypeEnum(enum.Enum):
    image = 'image'
    json = 'json'

class Object(Base):
    __tablename__ = "object"

    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column()
    task_id: Mapped[int] = mapped_column(ForeignKey("task.id"))
    algorithm_name: Mapped[str] = mapped_column(nullable=True)
    type: Mapped[str] = mapped_column(ENUM(*(e.value for e in ObjectTypeEnum), name='object_type_enum'))


# class Group(Base):
#     __tablename__ = "group"

#     id: Mapped[int] = mapped_column(primary_key=True)
#     name: Mapped[str] = mapped_column(nullable=True)

# class ObjectGroup(Base):
#     __tablename__ = "object_group"

#     id: Mapped[int] = mapped_column(primary_key=True)
#     name: Mapped[str] = mapped_column(nullable=True)


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


@app.get('/api/storage/{filepath:path}')
async def get_file(filepath: str):
    return fastapi.responses.FileResponse(filepath)


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


@app.post('/api/objects')
async def create_object(
    task_id: int,
    file: fastapi.UploadFile,
    type: Annotated[ObjectTypeEnum, fastapi.Form()],
    algorithm_name: Annotated[str, fastapi.Form()] = None,
):
    _, file_extension = os.path.splitext(file.filename)
    path = os.path.join('storage', f'{uuid.uuid4()}{file_extension}')

    with open(path, 'wb') as outfile:
        outfile.write(await file.read())

    async with async_session.begin() as session:
        object = Object(
            task_id=task_id,
            path=path,
            type=type.value,
            algorithm_name=algorithm_name
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
