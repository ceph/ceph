class MirrorException(Exception):
    def __init__(self, error_code, error_msg=''):
        super(MirrorException, self).__init__(error_code, error_msg)

    def __str__(self):
        return super(MirrorException, self).__str__()
