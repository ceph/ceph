import json

from cephadm.operations import OperationsRegistry
from cephadm.module import CephadmOrchestrator


class TestOperationsRegistry:

    def test_begin_update_complete(self, cephadm_module: CephadmOrchestrator):
        ops = cephadm_module.ops
        op_id = ops.begin_operation(
            kind='placement:update',
            title='Updating mgr according to the new mapping',
            service_name='mgr',
            service_type='mgr',
            progress_total=2,
        )
        ops.update_operation(op_id, current_step='Deploying mgr.a on host1', progress_done=1)
        ops.complete_operation(op_id)

        active = ops.list_operations(show_all=False)
        assert active == []

        all_ops = ops.list_operations(show_all=True)
        assert len(all_ops) == 1
        assert all_ops[0]['state'] == 'completed'
        assert all_ops[0]['progress_done'] == 1
        assert all_ops[0]['service_name'] == 'mgr'

    def test_fail_operation(self, cephadm_module: CephadmOrchestrator):
        ops = cephadm_module.ops
        op_id = ops.begin_operation(
            kind='service:remove',
            title='Removing service nfs.foo',
            service_name='nfs.foo',
            service_type='nfs',
            progress_total=1,
        )
        ops.fail_operation(op_id, 'boom')
        hist = ops.list_operations(show_all=True)
        assert hist[0]['state'] == 'failed'
        assert hist[0]['details']['error'] == 'boom'
        # active key cleared
        assert cephadm_module.get_store(f'cephadm/operations/{op_id}') is None

    def test_format_operations(self, cephadm_module: CephadmOrchestrator):
        ops = cephadm_module.ops
        op_id = ops.begin_operation(
            kind='placement:update',
            title='Updating mon according to the new mapping',
            service_name='mon',
            service_type='mon',
            progress_total=3,
        )
        ops.update_operation(op_id, current_step='Deploying mon.b on host2', progress_done=2)
        text = OperationsRegistry.format_operations(ops.list_operations())
        assert 'Updating mon according to the new mapping' in text
        assert 'Status: 2/3' in text
        assert 'Deploying mon.b on host2' in text

    def test_show_operations_via_module(self, cephadm_module: CephadmOrchestrator):
        cephadm_module.ops.begin_operation(
            kind='placement:update',
            title='Updating rgw.foo according to the new mapping',
            service_name='rgw.foo',
            service_type='rgw',
            progress_total=1,
        )
        result = cephadm_module.show_operations()
        assert len(result.result) == 1
        assert result.result[0]['kind'] == 'placement:update'

    def test_limit(self, cephadm_module: CephadmOrchestrator):
        ops = cephadm_module.ops
        for i in range(3):
            ops.begin_operation(
                kind='placement:update',
                title=f'op-{i}',
                service_name=f'svc{i}',
                service_type='nfs',
                progress_total=1,
            )
        assert len(ops.list_operations(limit=2)) == 2

    def test_persistence_roundtrip(self, cephadm_module: CephadmOrchestrator):
        ops = cephadm_module.ops
        op_id = ops.begin_operation(
            kind='placement:update',
            title='Updating crash according to the new mapping',
            service_name='crash',
            service_type='crash',
            progress_total=1,
        )
        raw = cephadm_module.get_store(f'cephadm/operations/{op_id}')
        assert raw is not None
        data = json.loads(raw)
        assert data['op_id'] == op_id
        idx = json.loads(cephadm_module.get_store(ops.INDEX_KEY))
        assert op_id in idx
