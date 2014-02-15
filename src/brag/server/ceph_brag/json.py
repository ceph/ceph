from pecan.jsonify import jsonify
from ceph_brag.model import db 

@jsonify.register(db.version_info)
def jsonify_version(vi):
    return dict(
            version_id=vi.index,
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
            bytes={'count':comps.byte_count, 'scale':comps.byte_scale},
            osds=comps.num_osds,
            objects=comps.num_objects,
            pgs=comps.num_pgs,
            pools=comps.num_pools,
            mdss=comps.num_mdss,
            mons=comps.num_mons
            )

@jsonify.register(db.pools_info)
def jsonify_pools_info(pool):
    return dict(rep_size=pool.pool_rep_size,
                name=pool.pool_name,
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
    crush_types=b.comps.crush_types.split(',')
    return dict(uuid=b.ci.uuid,
                cluster_creation_date=str(b.ci.cluster_creation_date),
                components_count=b.comps,
                crush_types=crush_types,
                ownership=ownership,
                pool_metadata=b.pools,
                sysinfo=b.osds
                )
