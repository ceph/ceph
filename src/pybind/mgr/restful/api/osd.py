from pecan import expose, request, response
from pecan.rest import RestController

from restful import common, context
from restful.decorators import auth


class OsdIdCommand(RestController):
    def __init__(self, osd_id):
        self.osd_id = osd_id


    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show implemented commands for the OSD id
        """
        osd = context.instance.get_osd_by_id(self.osd_id)

        if not osd:
            response.status = 500
            return {'message': 'Failed to identify the OSD id "%d"' % self.osd_id}

        if osd['up']:
            return common.OSD_IMPLEMENTED_COMMANDS
        else:
            return []


    @expose(template='json')
    @auth
    def post(self, **kwargs):
        """
        Run the implemented command for the OSD id
        """
        command = request.json.get('command', None)

        osd = context.instance.get_osd_by_id(self.osd_id)

        if not osd:
            response.status = 500
            return {'message': 'Failed to identify the OSD id "%d"' % self.osd_id}

        if not osd['up'] or command not in common.OSD_IMPLEMENTED_COMMANDS:
            response.status = 500
            return {'message': 'Command "%s" not available' % command}

        return context.instance.submit_request([[{
            'prefix': 'osd ' + command,
            'who': str(self.osd_id)
        }]], **kwargs)



class OsdId(RestController):
    def __init__(self, osd_id):
        self.osd_id = osd_id
        self.command = OsdIdCommand(osd_id)


    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for the OSD id
        """
        osd = context.instance.get_osds(ids=[str(self.osd_id)])
        if len(osd) != 1:
            response.status = 500
            return {'message': 'Failed to identify the OSD id "%d"' % self.osd_id}

        return osd[0]


    @expose(template='json')
    @auth
    def patch(self, **kwargs):
        """
        Modify the state (up, in) of the OSD id or reweight it
        """
        args = request.json

        commands = []

        if 'in' in args:
            if args['in']:
                commands.append({
                    'prefix': 'osd in',
                    'ids': [str(self.osd_id)]
                })
            else:
                commands.append({
                    'prefix': 'osd out',
                    'ids': [str(self.osd_id)]
                })

        if 'up' in args:
            if args['up']:
                response.status = 500
                return {'message': "It is not valid to set a down OSD to be up"}
            else:
                commands.append({
                    'prefix': 'osd down',
                    'ids': [str(self.osd_id)]
                })

        if 'reweight' in args:
            commands.append({
                'prefix': 'osd reweight',
                'id': self.osd_id,
                'weight': args['reweight']
            })

        return context.instance.submit_request([commands], **kwargs)



class Osd(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for all the OSDs
        """
        # Parse request args
        # TODO Filter by ids
        pool_id = kwargs.get('pool', None)

        return context.instance.get_osds(pool_id)


    @expose()
    def _lookup(self, osd_id, *remainder):
        return OsdId(int(osd_id)), remainder
