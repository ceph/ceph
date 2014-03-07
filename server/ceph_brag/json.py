from pecan.jsonify import jsonify
from ceph_brag.model import db 

@jsonify.register(db.version_info)
def jsonify_version(vi):
    return dict(
            version_number=vi.version_number,
            version_date=str(vi.version_date)
            )

@jsonify.register(db.cluster_info)
def jsonify_cluster_info(ci):
    return dict(
              uuid=ci.uuid,
              organization=ci.organization,
              email=ci.contact_email,
              cluster_name=ci.cluster_name,
              cluster_creation_date=str(ci.cluster_creation_date),
              num_versions=ci.num_versions
              )

@jsonify.register(db.components_info)
def jsonify_components_info(comps):
    return dict(
            num_bytes=db.bytes_raw_to_pretty(comps.num_bytes),
            num_osds=comps.num_osds,
            num_objects=comps.num_objects,
            num_pgs=comps.num_pgs,
            num_pools=comps.num_pools,
            num_mdss=comps.num_mdss,
            num_mons=comps.num_mons
            )

@jsonify.register(db.crush_types)
def jsonify_crush_types(crush):
    return dict(type=crush.crush_type,
                count=crush.crush_count)

@jsonify.register(db.pools_info)
def jsonify_pools_info(pool):
    return dict(size=pool.pool_rep_size,
                type=pool.pool_type,
                id=pool.pool_id
               )

@jsonify.register(db.osds_info)
def jsonify_osds_info(osd):
    return dict(nw_info={'address':osd.nw_address,'hostname':osd.hostname},
                hw_info={'swap_kb':osd.swap_kb,'mem_kb':osd.mem_kb,
                         'arch':osd.arch, 'cpu':osd.cpu},
                id=osd.osd_id,
                os_info={'os':osd.os,'version':osd.os_version,
                         'description':osd.os_desc, 'distro':osd.distro},
                ceph_version=osd.ceph_version
               )

@jsonify.register(db.brag)
def jsonify_brag(b):
    ownership = {'organization':b.ci.organization,
                 'description':b.ci.description,
                 'email':b.ci.contact_email,
                 'name':b.ci.cluster_name
                } 
    return dict(uuid=b.ci.uuid,
                cluster_creation_date=str(b.ci.cluster_creation_date),
                components_count=b.comps,
                crush_types=b.crush,
                ownership=ownership,
                pool_metadata=b.pools,
                sysinfo=b.osds
                )
