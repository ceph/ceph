from mgr_module import MgrModule


class OSDSupport(MgrModule):
    # Kept to keep upgrades from older point releases working.
    # This module can be removed as soon as we no longer
    # support upgrades from old octopus point releases.

    # On the other hand, if you find a use for this module,
    # Feel free to use it!

    COMMANDS = []

    MODULE_OPTIONS: []

    NATIVE_OPTIONS: []

    def __init__(self, *args, **kwargs):
        super(OSDSupport, self).__init__(*args, **kwargs)
