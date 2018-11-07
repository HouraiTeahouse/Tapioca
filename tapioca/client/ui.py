import asyncio
import aiohttp
import logging

log = logging.getLogger(__name__)


class ClientState(Enum):
    # Game is ready to play and launch
    READY = 0
    # Checking for updates for the base launcher/updater
    LAUNCHER_UPDATE_CHECK = 1
    # Launcher needs to update
    LAUNCHER_UPDATE = 2
    # Checking the status of the local files
    GAME_STATUS_CHECK = 3
    # Checking the status of the local
    GAME_UPDATE_CHECK = 4
    # Pending game update
    PENDING_GAME_UPDATE = 5
    # Game is downloading needed new files for update
    GAME_UPDATE = 6
    # Game update errored out, need to restart patching process
    GAME_UPDATE_ERROR = 7


class MainWindow():

    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.session = aiohttp.ClientSession(loop=self.loop)

    async def __aenter__(self):
        await self.session.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self.session.__aenter__(*args)

    async def main_loop(self):

