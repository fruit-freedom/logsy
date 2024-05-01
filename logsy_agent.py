import importlib
import asyncio
import traceback

import fastapi
import pydantic

from logsy import Task


app = fastapi.FastAPI()


async def perform_run_task(task_id: int, entry_point: str):
    module = importlib.import_module(f'.{entry_point.split(".")[0]}', 'storage')

    try:
        task = Task(id=task_id)
        await module.main(task)
    except BaseException:
        stacktrace = traceback.format_exc()
        print(stacktrace)
        await task.set_exception(stacktrace)


class RunTaskRequest(pydantic.BaseModel):
    task_id: int
    entry_point: str


@app.post('/api/tasks/run')
async def run_task(body: RunTaskRequest, background_tasks: fastapi.BackgroundTasks):
    print(background_tasks)
    background_tasks.add_task(perform_run_task, body.task_id, body.entry_point)


async def main():
    # module = importlib.import_module('task')

    entrypoint = 'src_efc4c716.py'
    module = importlib.import_module(f'.{entrypoint.split(".")[0]}', 'storage')

    try:
        task = await Task.init()
        await module.main(task)
    except BaseException:
        stacktrace = traceback.format_exc()
        print(stacktrace)
        await task.set_exception(stacktrace)


if __name__ == '__main__':
    asyncio.run(main())
