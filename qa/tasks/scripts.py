import os

from util import copy_directory_recursively

copied = False


class Scripts:

    def __init__(self, remotes):
        global copied
        if not copied:
            local_path = os.path.dirname(os.path.realpath(__file__)) + '/scripts/'
            for remote_name, remote_obj in remotes.items():
                copy_directory_recursively(local_path, remote_obj, "scripts")
            copied = True

    def run(self, remote, script_name, args=[], as_root=True):
        path = 'scripts/' + script_name
        cmd = 'bash {}'.format(path)
        if as_root:
            cmd = "sudo " + cmd
        if args:
            cmd += ' ' + ' '.join(args)
        remote.run(label=script_name, args=cmd)
