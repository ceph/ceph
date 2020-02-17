from mgr_module import Option, Command

from . import PLUGIN_MANAGER as PM
from . import interfaces as I  # noqa: E741,N812


class SimplePlugin(I.CanMgr, I.HasOptions, I.HasCommands):
    """
    Helper class that provides simplified creation of plugins:
        - Default Mixins/Interfaces: CanMgr, HasOptions & HasCommands
    - Options are defined by OPTIONS class variable, instead from get_options hook
    - Commands are created with by COMMANDS list of Commands() and handlers
    (less compact than CLICommand, but allows using method instances)
    """
    Option = Option
    Command = Command

    @PM.add_hook
    def get_options(self):
        return self.OPTIONS

    @PM.final
    def get_option(self, option):
        return self.mgr.get_module_option(option)

    @PM.final
    def set_option(self, option, value):
        self.mgr.set_module_option(option, value)

    @PM.add_hook
    def register_commands(self):
        for cmd in self.COMMANDS:
            cmd.register(instance=self)
