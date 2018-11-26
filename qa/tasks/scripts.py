import os

from util import copy_directory_recursively


class Scripts:

    def __init__(self, ctx, logger, remotes):
        self.log = logger
        copied = ctx.get('scripts_copied', False)
        if copied:
            # self.log.info('(scripts ctor) scripts already copied to remotes')
            pass
        else:
            local_path = os.path.dirname(os.path.realpath(__file__)) + '/scripts/'
            for remote_name, remote_obj in remotes.items():
                copy_directory_recursively(local_path, remote_obj, "scripts")
            ctx['scripts_copied'] = True

    def run(self, remote, script_name, args=[], as_root=True):
        self.log.info('(scripts) running script {} on remote {}'
                      .format(script_name, remote.hostname)
                      )
        path = 'scripts/' + script_name
        cmd = 'bash {}'.format(path)
        if as_root:
            cmd = "sudo " + cmd
        if args:
            cmd += ' ' + ' '.join(args)
        remote.run(label=script_name, args=cmd)
