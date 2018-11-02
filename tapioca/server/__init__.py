from aiohttp import web
from tapioca.server.util import BuildDeployment
import asyncio
import tapioca.deploy.config as config
import tapioca.server.db as db
import uvloop


routes = web.RoutTableDef()


@routes.get('/build/{project}/{target}')
@routes.get('/build/{project}/{target}/{branch}')
@routes.get('/build/{project}/{target}/{branch}/{build}')
async def get_build(request):
    build_deployment = BuildDeployment.from_http_request(request)
    build = db.get_manifest(build_deployment)

    if build is not None:
        return web.Response(body=build.SerializeAsString())
    else:
        return web.Response(status=404)


@routes.post('/deploy/{handler}/{project}/{branch}')
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
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    db.init()

    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, *args, **kwargs)
