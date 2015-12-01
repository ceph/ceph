from teuthology.config import FakeNamespace
from teuthology.task import internal


class TestInternal(object):
    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.config = dict()

    def test_buildpackages_prep(self):
        #
        # no buildpackages nor install tasks
        #
        self.ctx.config = { 'tasks': [] }
        assert internal.buildpackages_prep(self.ctx,
                                           self.ctx.config) == internal.BUILDPACKAGES_NOTHING
        #
        # make the buildpackages tasks the first to run
        #
        self.ctx.config = {
            'tasks': [ { 'atask': None },
                       { 'internal.buildpackages_prep': None },
                       { 'btask': None },
                       { 'install': None },
                       { 'buildpackages': None } ],
        }
        assert internal.buildpackages_prep(self.ctx,
                                           self.ctx.config) == internal.BUILDPACKAGES_FIRST
        assert self.ctx.config == {
            'tasks': [ { 'atask': None },
                       { 'internal.buildpackages_prep': None },
                       { 'buildpackages': None },
                       { 'btask': None },
                       { 'install': None } ],
        }
        #
        # the buildpackages task already the first task to run
        #
        assert internal.buildpackages_prep(self.ctx,
                                           self.ctx.config) == internal.BUILDPACKAGES_OK
        #
        # no buildpackages task
        #
        self.ctx.config = {
            'tasks': [ { 'install': None } ],
        }
        assert internal.buildpackages_prep(self.ctx,
                                           self.ctx.config) == internal.BUILDPACKAGES_NOTHING
        #
        # no install task: the buildpackages task must be removed
        #
        self.ctx.config = {
            'tasks': [ { 'buildpackages': None } ],
        }
        assert internal.buildpackages_prep(self.ctx,
                                           self.ctx.config) == internal.BUILDPACKAGES_REMOVED
        assert self.ctx.config == {'tasks': []}
