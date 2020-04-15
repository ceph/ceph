import re

GLOBAL_POOL_KEY = (None, None)

class NotAuthorizedError(Exception):
    pass


def is_authorized(module, pool, namespace):
    return module.is_authorized({"pool": pool or '',
                                 "namespace": namespace or ''})


def authorize_request(module, pool, namespace):
    if not is_authorized(module, pool, namespace):
        raise NotAuthorizedError("not authorized on pool={}, namespace={}".format(
            pool, namespace))


def extract_pool_key(pool_spec):
    if not pool_spec:
        return GLOBAL_POOL_KEY

    match = re.match(r'^([^/]+)(?:/([^/]+))?$', pool_spec)
    if not match:
        raise ValueError("Invalid pool spec: {}".format(pool_spec))
    return (match.group(1), match.group(2) or '')


def get_rbd_pools(module):
    osd_map = module.get('osd_map')
    return {pool['pool']: pool['pool_name'] for pool in osd_map['pools']
            if 'rbd' in pool.get('application_metadata', {})}

