from pecan import expose, request
from pecan.rest import RestController

from restful import common, context
from restful.decorators import auth


class ConfigOsd(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show OSD configuration options
        """
        flags = context.instance.get("osd_map")['flags']

        # pause is a valid osd config command that sets pauserd,pausewr
        flags = flags.replace('pauserd,pausewr', 'pause')

        return flags.split(',')


    @expose(template='json')
    @auth
    def patch(self, **kwargs):
        """
        Modify OSD configuration options
        """
        args = request.json

        commands = []

        valid_flags = set(args.keys()) & set(common.OSD_FLAGS)
        invalid_flags = list(set(args.keys()) - valid_flags)
        if invalid_flags:
            context.instance.log.warn("%s not valid to set/unset", invalid_flags)

        for flag in list(valid_flags):
            if args[flag]:
                mode = 'set'
            else:
                mode = 'unset'

            commands.append({
                'prefix': 'osd ' + mode,
                'key': flag,
            })

        return context.instance.submit_request([commands], **kwargs)



class ConfigClusterKey(RestController):
    def __init__(self, key):
        self.key = key


    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show specific configuration option
        """
        return context.instance.get("config").get(self.key, None)



class ConfigCluster(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show all cluster configuration options
        """
        return context.instance.get("config")


    @expose()
    def _lookup(self, key, *remainder):
        return ConfigClusterKey(key), remainder



class Config(RestController):
    cluster = ConfigCluster()
    osd = ConfigOsd()
