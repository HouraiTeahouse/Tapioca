import platform

def get_targets():
    """ Enumerates all of the valid targets for the current machine. """
    # TODO(james7132):
    plat = platform.system()
    if "darwin" in plat.lower():
        plat = "OSX"
    return plat
