import asyncio
import logging
import os
import sys
import uvloop

# Use uvloop where possible
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logging.basicConfig(level=logging.DEBUG,
                    # format='%(asctime)s %(levelname)s %(message)s',
                    filename='tapioca.log',
                    filemode='w')

RESOURCE_DIR = None

# Server configuration
SERVER_PORT = None
SERVER_SOCKET = None


def _common_configure():
    module = sys.module[__name__]
    for env, value in os.environ.items():
        try:
          if hasattr(module, env):
              setattr(module, env, value)
        except Exception:
            # TODO(james7132): Handle these errors
            pass


def configure_client():
    _common_configure()


def configure_server(socket, port):
    _common_configure()
    SERVER_SOCKET = socket or SERVER_SOCKET
    SERVER_PORT = port or SERVER_PORT
