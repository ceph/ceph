class MirrorException(Exception):
    def __init__(self, error_code, error_msg=''):
        super(Exception, self).__init__(error_code, error_msg)

    def __str__(self):
        return super(Exception, self).__str__()
