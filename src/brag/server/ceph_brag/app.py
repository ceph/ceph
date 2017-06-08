from pecan import make_app
from ceph_brag import model, json
from pecan.hooks import TransactionHook

def setup_app(config):

    model.init_model()
    app_conf = dict(config.app)

    return make_app(
        app_conf.pop('root'),
        logging=getattr(config, 'logging', {}),
        hooks=[TransactionHook(model.start,
                               model.start,
                               model.commit, 
                               model.rollback,
                               model.clear)],
        **app_conf
    )
