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

async def t(task: Task):
    from algorithm_service import tree_segmentor

    masks = await tree_segmentor.inference(
        task.inputs['ortomosaic_path']
    )

    for mask in mask:
        task.log_geotiff(masks)

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

async def main(task: Task):
    # await task.log_xyz('tiles', 'Source image', meta={
    #     'tile_size': [256, 256],
    #     'center': [5452545.576071, 7537291.215104],
    #     'min_zoom': 15,
    #     'max_zoom': 21,
    #     'extent': [5451970.828121, 7536839.756483,5453120.324020, 7537742.673725]
    # })
    await task.log_image('image.jpg', 'Source image')
    group = await task.create_group('layers')
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

    # await task.log_geotiff(algorithm_name='Source image', file=open('orto.tif', 'rb'))
    await task.set_result()

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
