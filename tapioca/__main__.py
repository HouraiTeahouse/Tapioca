import click


@click.group()
def cli():
    pass


@cli.group()
def run():
    pass


@run.command()
@cli.option('path', help='The socket path for the server to bind to.')
@cli.option('port', help='The network port for the server to bind to.')
def deploy_server(path, port):
    import tapioca.deploy as deploy
    deploy.run_server(path=path, port=port)


cli()
