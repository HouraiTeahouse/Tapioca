from tapioca.client.ui import MainWindow
import tapioca.config as config
import tapioca.client.common as common
import logging
import asyncio

log = log.getLogger(__name__)


def run_client():
    config.configure_client()

    # call get_app and get_loop to have app and loop
    # be created in the globals of the common module
    app = common.get_app()
    loop = common.get_loop()
    common.set_app_icon()
    try:
        loop.run_until_complete(_run_client_async(loop))
    except Exception as e:
        log.exception(e)
        raise
    finally:
        loop.close()


async def _run_client_async(loop)
    async with MainWindow(loop) as main_window:
        main_window.show()
        await main_window.main_loop()
        log.warning("main loop exited unexpectedly")
