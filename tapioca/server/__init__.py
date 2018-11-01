from aiohttp import web
from tapioca.server.util import BuildDeployment
import asyncio
import tapioca.deploy.config as config
import tapioca.server.db as db


routes = web.RoutTableDef()


@routes.post('/manifest/{project}')
@routes.post('/manifest/{project}/{branch}')
@routes.post('/manifest/{project}/{branch}/{build}')
async def manifest(request):
    build = BuildDeployment.from_http_request(request)
    manifest_bytes = db.get_manifest(build)

    if manifest_bytes is not None:
        return web.Response(status=200, body=manifest_bytes)
    else:
        return web.Response(status=404)


@routes.post('/deploy/{handler}/{project}')
@routes.post('/deploy/{handler}/{project}/{branch}')
@routes.post('/deploy/{handler}/{project}/{branch}/{build}')
async def deploy(request):
    # TODO(james7132): Authenticate

    deployment_handlers = config.HANDLERS.get(request.match_info['handler'])
    if deployment_handlers is None:
        return web.json_response({
            'status': 404,
            'message': 'No such deployment handler.'
        }, status=404)

    build = BuildDeployment.from_http_request(request)

    await asyncio.gather(*[
        handler.run(build) for handler in deployment_handlers
        if handler is not None
    ])

    return web.json_response({'message': 'Successfully deployed build.'})


def run_server(*args, **kwargs):
    db.init()

    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, *args, **kwargs)
