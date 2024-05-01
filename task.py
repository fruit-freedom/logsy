import asyncio
import requests
from logsy import Task

async def main(task: Task):
    print('[Task] Starting task')
    await task.log_json({ 'mask': [1, 2, 3] }, 'waste-fraction-segmentor')
    await asyncio.sleep(0.4)

    # raise Exception('')

    await task.log_json({ 'geometry': [] }, 'waste-postprocess')
    await task.log_json({ 'categories': [], 'mask': [], 'mask_width': 1920, 'mask_height': 1080 }, 'segm')
    await task.set_result()

    # task.binded_log(
    #     Log(mask), Log(image)
    # )



if __name__ == '__main__':
    async def async_launch():
        await main(await Task.init({
            'ortomosaic': 'kazan.tif',
            'geojsons': [
                'kazan-body.geojson',
                'kazan-cadastr.geojson'
            ],
        }))

    asyncio.run(async_launch())
