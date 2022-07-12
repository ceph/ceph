import copy

from teuthology import misc
from teuthology.orchestra import run

class Refspec:
    def __init__(self, refspec):
        self.refspec = refspec

    def __str__(self):
        return self.refspec

    def _clone(self, git_url, clonedir, opts=None):
        if opts is None:
            opts = []
        return (['rm', '-rf', clonedir] +
                [run.Raw('&&')] +
                ['git', 'clone'] + opts +
                [git_url, clonedir])

    def _cd(self, clonedir):
        return ['cd', clonedir]

    def _checkout(self):
        return ['git', 'checkout', self.refspec]

    def clone(self, git_url, clonedir):
        return (self._clone(git_url, clonedir) +
                [run.Raw('&&')] +
                self._cd(clonedir) +
                [run.Raw('&&')] +
                self._checkout())


class Branch(Refspec):
    def __init__(self, tag):
        Refspec.__init__(self, tag)

    def clone(self, git_url, clonedir):
        opts = ['--depth', '1',
                '--branch', self.refspec]
        return (self._clone(git_url, clonedir, opts) +
                [run.Raw('&&')] +
                self._cd(clonedir))


class Head(Refspec):
    def __init__(self):
        Refspec.__init__(self, 'HEAD')

    def clone(self, git_url, clonedir):
        opts = ['--depth', '1']
        return (self._clone(git_url, clonedir, opts) +
                [run.Raw('&&')] +
                self._cd(clonedir))


def get_refspec_after_overrides(config, overrides):
    # mimic the behavior of the "install" task, where the "overrides" are
    # actually the defaults of that task. in other words, if none of "sha1",
    # "tag", or "branch" is specified by a "workunit" tasks, we will update
    # it with the information in the "workunit" sub-task nested in "overrides".
    overrides = copy.deepcopy(overrides.get('workunit', {}))
    refspecs = {'suite_sha1': Refspec, 'suite_branch': Branch,
                'sha1': Refspec, 'tag': Refspec, 'branch': Branch}
    if any(map(lambda i: i in config, refspecs.keys())):
        for i in refspecs.keys():
            overrides.pop(i, None)
    misc.deep_merge(config, overrides)

    for spec, cls in refspecs.items():
        refspec = config.get(spec)
        if refspec:
            refspec = cls(refspec)
            break
    if refspec is None:
        refspec = Head()
    return refspec
