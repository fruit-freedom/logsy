import asyncio
import requests
from logsy import Task

async def main(task: Task):
    print('[Task] Starting task')
    await task.log_json({ 'mask': [1, 2, 3] }, 'waste-fraction-segmentor')
    await asyncio.sleep(0.4)

    # raise Exception('')

    await task.log_json({ 'report': { 'violation': True, 'lat': 65.124214, 'lon': 62.5235 } }, 'waste-postprocess')
    await task.log_json({ 'categories': [], 'mask': [], 'mask_width': 1920, 'mask_height': 1080 }, 'segm')
    await task.set_result()

async def main(task: Task):
    print('[Task] Starting task')
    await task.log_image('image.jpg', 'Source image')
    await task.log_json([
        {
            'category_name': 'insulator',
            'confidence': 0.89,
            'bbox': {
                'width': 0.523,
                'height': 0.623,
                'x': 0.1,
                'y': 0.214
            }
        },
        {
            'category_name': 'insulator',
            'confidence': 0.981,
            'bbox': {
                'width': 0.123,
                'height': 0.276,
                'x': 0.54,
                'y': 0.79
            }
        }
    ], 'waste-fraction-segmentor')

    await asyncio.sleep(0.4)

    await task.log_json({ 'categories': [], 'mask': [], 'mask_width': 1920, 'mask_height': 1080 }, 'segm')
    await task.log_json({ 'report': { 'violation': True, 'lat': 65.124214, 'lon': 62.5235 } }, 'waste-postprocess')
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
