import configparser
import logging

from . import utils

class Ctx():
    # TODO: typing.NamedTuple or bunch would make sense here
    def __init__(self, auth_type, log_level, arg_list, auth_creds,
                 base_url, response_handler=None, buckets=[], admin_sock_path=None):
        self.auth_type = auth_type
        self.log_level = utils.str_to_log_level(log_level)
        self.arg_list = arg_list
        self.auth_creds = auth_creds
        self.base_url = base_url
        self.response_handler = response_handler
        self.buckets = buckets
        self.admin_sock_path = admin_sock_path

    def set_up_logging(self):
        logging.basicConfig(level=self.log_level)
        logging.getLogger('asyncio').setLevel(self.log_level)
        logging.getLogger('botocore').setLevel(self.log_level)
        logging.getLogger('async-client').setLevel(self.log_level)
        logging.getLogger('admin-sock').setLevel(self.log_level)

def make_ctx(conffile):
    cfg = configparser.ConfigParser()
    cfg.read_file(open(conffile))

    baseurl = cfg['DEFAULT']['base_url']
    base_req_count = cfg['DEFAULT'].getint('req_count',100)
    loglevel = cfg['DEFAULT'].get('log_level','INFO')
    auth_type = cfg['DEFAULT'].get('auth_type','s3')
    resp_handler = cfg['DEFAULT'].get('response_handler', None)
    admin_sock_path = cfg['DEFAULT'].get('admin_sock_path', None)
    auth_creds = dict()
    if auth_type.lower() == 's3':
        auth_creds['access_key'] = cfg['DEFAULT']['access_key']
        auth_creds['secret_key'] = cfg['DEFAULT']['secret_key']

    create_bucket_args = []
    buckets_str = cfg['DEFAULT'].get('create_buckets', "").strip('" ')
    buckets = [bucket.strip() for bucket in buckets_str.split(",")]
    args_lst = []
    for section in cfg.sections():
        d = {}
        d['req_type'] = cfg[section].get('req_type')
        d['req_url'] = utils.normalise_url_path(baseurl, cfg[section].get('req_path'))
        d['per_req_path'] = cfg[section].getboolean('per_req_path', False)
        d['req_count'] = cfg[section].get('req_count',base_req_count)
        sz = cfg[section].getint('obj_size',0)
        if sz > 0:
            bufferv = utils.create_buffer(sz)
            d['data'] = bufferv
        args_lst.append(d)
    return Ctx(auth_type, loglevel, args_lst, auth_creds,
               baseurl, resp_handler, buckets, admin_sock_path)
