from ceph.mon_command_api import MonCommandApi

def osd_empty(osd_id):
    pass

def remove_osd(osd_id, rados):
    # Would this work?
    # Does this make sense?
    # This looks like it should live in the orchestrator.py instead?
    api = MonCommandApi(rados)
    api.osd_out([osd_id])
    osd_empty(osd_id)
    api.osd_purge(osd_id)
    api.orchestrator_service_instance('stop', svc_type='osd', svc_id=osd_id)
    api.orchestrator_osd_zap(osd_id)
