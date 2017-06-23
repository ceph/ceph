

class SuffixParsingError(Exception):

    def __init__(self, suffix, part=None):
        self.suffix = suffix
        self.part = part

    def __str__(self):
        return 'Unable to parse the %s from systemd suffix: %s' % (self.part, self.suffix)


class SuperUserError(Exception):

    def __str__(self):
        return 'This command needs to be executed with sudo or as root'
