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
            num_data_bytes=comps.num_data_bytes,
            num_bytes_total=comps.num_bytes_total,
            num_osds=comps.num_osds,
            num_objects=comps.num_objects,
            num_pgs=comps.num_pgs,
            num_pools=comps.num_pools,
            num_mdss=comps.num_mdss,
            num_mons=comps.num_mons)

@jsonify.register(db.crush_types)
def jsonify_crush_types(crush):
    return dict(type=crush.crush_type,
                count=crush.crush_count)

@jsonify.register(db.pools_info)
def jsonify_pools_info(pool):
    return dict(size=pool.pool_rep_size,
                type=pool.pool_type,
                id=pool.pool_id)

@jsonify.register(db.os_info)
def jsonify_os_info(value):
    return dict(os=value.os,
                count=value.count)

@jsonify.register(db.kernel_versions)
def jsonify_kernel_versions(value):
    return dict(version=value.version,
                count=value.count)

@jsonify.register(db.kernel_types)
def jsonify_kernel_types(value):
    return dict(type=value.type,
                count=value.count)

@jsonify.register(db.distros)
def jsonify_distros(value):
    return dict(distro=value.distro,
                count=value.count)

@jsonify.register(db.cpus)
def jsonify_cpus(value):
    return dict(cpu=value.cpu,
                count=value.count)

@jsonify.register(db.cpu_archs)
def jsonify_cpu_archs(value):
    return dict(arch=value.arch,
                count=value.count)

@jsonify.register(db.ceph_versions)
def jsonify_ceph_versions(value):
    return dict(version=value.version,
                count=value.count)

@jsonify.register(db.sysinfo)
def jsonify_sysinfo(value):
    retval = {}
    
    if value.os:
      retval['os_info'] = value.os
    if value.kern_vers:
      retval['kernel_versions'] = value.kern_vers
    if value.kern_types:
      retval['kernel_types'] = value.kern_types
    if value.distros:
      retval['distros'] = value.distros
    if value.cpus:
      retval['cpus'] = value.cpus
    if value.cpu_archs:
      retval['cpu_archs'] = value.cpu_archs
    if value.ceph_vers:
      retval['ceph_versions'] = value.ceph_vers

    return retval

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
                sysinfo=b.sysinfo
                )
