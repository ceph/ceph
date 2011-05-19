import logging

log = logging.getLogger(__name__)

def patch_001_paramiko_deprecation():
    """
    Silence an an unhelpful DeprecationWarning triggered by Paramiko.

    Not strictly a monkeypatch.
    """
    import warnings
    warnings.filterwarnings(
        category=DeprecationWarning,
        message='This application uses RandomPool,',
        action='ignore',
        )


def patch_100_paramiko_pkey():
    """
    Make paramiko recognize AES-encrypted SSH private keys.

    http://comments.gmane.org/gmane.comp.python.paramiko/476
    """
    import paramiko.pkey
    import Crypto.Cipher
    assert 'AES-128-CBC' not in paramiko.pkey.PKey._CIPHER_TABLE
    paramiko.pkey.PKey._CIPHER_TABLE['AES-128-CBC'] = { 'cipher': Crypto.Cipher.AES, 'keysize': 16, 'blocksize': 16, 'mode': Crypto.Cipher.AES.MODE_CBC }


def patch_100_paramiko_log():
    """
    Silence some noise paramiko likes to log.

    Not strictly a monkeypatch.
    """
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)


def patch_100_logger_getChild():
    """
    Imitate Python 2.7 feature Logger.getChild.
    """
    import logging
    if not hasattr(logging.Logger, 'getChild'):
        def getChild(self, name):
            return logging.getLogger('.'.join([self.name, name]))
        logging.Logger.getChild = getChild


def patch_all():
    """
    Run all the patch_* functions in this module.
    """
    monkeys = [(k,v) for (k,v) in globals().iteritems() if k.startswith('patch_') and k != 'patch_all']
    monkeys.sort()
    for k,v in monkeys:
        log.debug('Patching %s', k)
        v()
