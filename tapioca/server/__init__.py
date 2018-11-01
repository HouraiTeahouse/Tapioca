import asyncio
from aiohttp import web
from collections import namedtuple
import tapioca.server.db as db
import tapioca.deploy.config as config

DeploymentRequest = namedtuple("DeploymentRequest",
                               "handler project branch build http_request")

routes = web.RoutTableDef()


@routes.post('/manifest/{project}')
@routes.post('/manifest/{project}/{branch}')
@routes.post('/manifest/{project}/{branch}/{build}')
async def manifest(request):
    manifest_bytes = db.get_manifest(
        request.match_info['project'],
        request.match_info['branch'],
        request.match_info['build'])

    if manifest_bytes is not None:
        return web.Response(status=200, body=manifest_bytes)
    else:
        return web.Response(status=404)


@routes.post('/deploy/{handler}/{project}')
@routes.post('/deploy/{handler}/{project}/{branch}')
@routes.post('/deploy/{handler}/{project}/{branch}/{build}')
async def deploy(request):
    parameters = request.match_info

    handler = parameters['handler']
    project = parameters['project']
    branch = parameters.get('branch', 'master')
    build = parameters.get('build')

    # TODO(james7132): Authenticate

    deployment_handlers = config.HANDLERS.get(handler)
    if deployment_handlers is None:
        response = web.json_response({
            'status': 404,
            'message': 'No such deployment handler.'
        })
        return response

    deploy_tasks = []
    for deployment_handler in deployment_handlers:
        if deployment_handler is None:
            continue
        deployment_request = DeploymentRequest(handler=handler,
                                               project=project,
                                               branch=branch,
                                               build=build,
                                               http_request=request)
        deploy_tasks.append(deployment_handler.run(deployment_request))
    await asyncio.gather(*deploy_tasks)

    return web.json_response({
        'message': 'Successfully deployed build.'
    })


def run_server(*args, **kwargs):
    db.init()

    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, *args, **kwargs)
