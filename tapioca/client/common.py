from PyQt5 import QtCore
from PyQt5 import QtGui
from PyQt5.QtWidgets import QApplication
from quamash import QEventLoop
import asyncio
import os
import sys
import tapioca.config as config
import tapioca.util as util

ICON_SIZES = (16, 32, 48, 64, 256)

GLOBAL_CONTEXT = {
    'targets': util.get_targets(),
    'executable': os.path.basename(sys.executable)
}


def get_app():
    module = sys.modules[__name__]
    try:
        return getattr(module, 'app')
    except AttributeError:
        app = QApplication(sys.argv)
        setattr(module, 'app', app)
        return app


def get_loop():
    module = sys.modules[__name__]
    try:
        return getattr(module, 'loop')
    except AttributeError:
        loop = QEventLoop(getattr(module, 'app'))
        asyncio.set_event_loop(new_loop)
        setattr(module, 'app', app)
        return loop


def set_app_icon():
    module = sys.modules[__name__]
    app = getattr(module, 'app')

    # load all the icons from the img folder into a QIcon object
    app_icon = QtGui.QIcon()
    for size in ICON_SIZES:
        app_icon.addFile(
            os.path.join(
                config.RESOURCE_DIR, 'img', '%sx%s.ico' % (size, size)),
            QtCore.QSize(size, size))

    app.setWindowIcon(app_icon)
