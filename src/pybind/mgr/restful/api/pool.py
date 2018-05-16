from pecan import expose, request, response
from pecan.rest import RestController

from restful import common, context
from restful.decorators import auth


class PoolId(RestController):
    def __init__(self, pool_id):
        self.pool_id = pool_id


    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for the pool id
        """
        pool = context.instance.get_pool_by_id(self.pool_id)

        if not pool:
            response.status = 500
            return {'message': 'Failed to identify the pool id "%d"' % self.pool_id}

        # pgp_num is called pg_placement_num, deal with that
        if 'pg_placement_num' in pool:
            pool['pgp_num'] = pool.pop('pg_placement_num')
        return pool


    @expose(template='json')
    @auth
    def patch(self, **kwargs):
        """
        Modify the information for the pool id
        """
        try:
            args = request.json
        except ValueError:
            response.status = 400
            return {'message': 'Bad request: malformed JSON or wrong Content-Type'}

        # Get the pool info for its name
        pool = context.instance.get_pool_by_id(self.pool_id)
        if not pool:
            response.status = 500
            return {'message': 'Failed to identify the pool id "%d"' % self.pool_id}

        # Check for invalid pool args
        invalid = common.invalid_pool_args(args)
        if invalid:
            response.status = 500
            return {'message': 'Invalid arguments found: "%s"' % str(invalid)}

        # Schedule the update request
        return context.instance.submit_request(common.pool_update_commands(pool['pool_name'], args), **kwargs)


    @expose(template='json')
    @auth
    def delete(self, **kwargs):
        """
        Remove the pool data for the pool id
        """
        pool = context.instance.get_pool_by_id(self.pool_id)

        if not pool:
            response.status = 500
            return {'message': 'Failed to identify the pool id "%d"' % self.pool_id}

        return context.instance.submit_request([[{
            'prefix': 'osd pool delete',
            'pool': pool['pool_name'],
            'pool2': pool['pool_name'],
            'sure': '--yes-i-really-really-mean-it'
        }]], **kwargs)



class Pool(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for all the pools
        """
        pools = context.instance.get('osd_map')['pools']

        # pgp_num is called pg_placement_num, deal with that
        for pool in pools:
            if 'pg_placement_num' in pool:
                pool['pgp_num'] = pool.pop('pg_placement_num')

        return pools


    @expose(template='json')
    @auth
    def post(self, **kwargs):
        """
        Create a new pool
        Requires name and pg_num dict arguments
        """
        args = request.json

        # Check for the required arguments
        pool_name = args.pop('name', None)
        if pool_name is None:
            response.status = 500
            return {'message': 'You need to specify the pool "name" argument'}

        pg_num = args.pop('pg_num', None)
        if pg_num is None:
            response.status = 500
            return {'message': 'You need to specify the "pg_num" argument'}

        # Run the pool create command first
        create_command = {
            'prefix': 'osd pool create',
            'pool': pool_name,
            'pg_num': pg_num
        }

        # Check for invalid pool args
        invalid = common.invalid_pool_args(args)
        if invalid:
            response.status = 500
            return {'message': 'Invalid arguments found: "%s"' % str(invalid)}

        # Schedule the creation and update requests
        return context.instance.submit_request(
            [[create_command]] +
            common.pool_update_commands(pool_name, args),
            **kwargs
        )


    @expose()
    def _lookup(self, pool_id, *remainder):
        return PoolId(int(pool_id)), remainder
