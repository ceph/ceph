import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import Column, Integer, String, \
     DateTime, ForeignKey, BigInteger
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr

Base = declarative_base()
Session = scoped_session(sessionmaker())

class cluster_info(Base):
  __tablename__ = 'cluster_info'

  index = Column(Integer, primary_key=True)
  uuid = Column(String(36), unique=True)
  organization = Column(String(64))
  contact_email = Column(String(32))
  cluster_name = Column(String(32))
  cluster_creation_date = Column(DateTime)
  description = Column(String(32))
  num_versions = Column(Integer)

class version_info(Base):
  __tablename__ = 'version_info'

  index = Column(Integer, primary_key=True)
  cluster_id = Column(ForeignKey('cluster_info.index'))
  version_number = Column(Integer)
  version_date = Column(DateTime)

class components_info(Base):
  __tablename__ = 'components_info'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  num_data_bytes = Column(BigInteger)
  num_bytes_total = Column(BigInteger)
  num_osds = Column(Integer)
  num_objects = Column(Integer)
  num_pgs = Column(Integer)
  num_pools = Column(Integer)
  num_mdss = Column(Integer)
  num_mons = Column(Integer)

class crush_types(Base):
  __tablename__ = 'crush_types'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  crush_type = Column(String(16))
  crush_count = Column(Integer)

class pools_info(Base):
  __tablename__ = 'pools_info'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  pool_id = Column(Integer)
  pool_type = Column(Integer)
  pool_rep_size = Column(Integer)

class os_info(Base):
  __tablename__ = 'os_info'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  os = Column(String(16))
  count = Column(Integer)

class kernel_versions(Base):
  __tablename__ = 'kernel_versions'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  version = Column(String(16))
  count = Column(Integer)

class kernel_types(Base):
  __tablename__ = 'kernel_types'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  type = Column(String(64))
  count = Column(Integer)

class distros(Base):
  __tablename__ = 'distros'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  distro = Column(String(64))
  count = Column(Integer)

class cpus(Base):
  __tablename__ = 'cpus'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  cpu = Column(String(16))
  count = Column(Integer)

class cpu_archs(Base):
  __tablename__ = 'cpu_archs'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  arch = Column(String(16))
  count = Column(Integer)

class ceph_versions(Base):
  __tablename__ = 'ceph_versions'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  version = Column(String(16))
  count = Column(Integer)

class sysinfo(object):
  def __init__(self, vindex):
    self.os = Session.query(os_info).filter_by(vid=vindex).all()
    self.kern_vers = Session.query(kernel_versions).filter_by(vid=vindex).all()
    self.kern_types = Session.query(kernel_types).filter_by(vid=vindex).all()
    self.distros = Session.query(distros).filter_by(vid=vindex).all()
    self.cpus = Session.query(cpus).filter_by(vid=vindex).all()
    self.cpu_archs = Session.query(cpu_archs).filter_by(vid=vindex).all()
    self.ceph_vers = Session.query(ceph_versions).filter_by(vid=vindex).all()

class brag(object):
  def __init__(self, uuid, version_number):
    self.ci = Session.query(cluster_info).filter_by(uuid=uuid).first()
    if self.ci is not None:
      self.vi = Session.query(version_info).filter_by(cluster_id=self.ci.index, version_number=version_number).first()
    
    if self.ci is not None and self.vi is not None:
      self.comps = Session.query(components_info).filter_by(vid=self.vi.index).first()
      self.crush = Session.query(crush_types).filter_by(vid=self.vi.index).all()
      self.pools = Session.query(pools_info).filter_by(vid=self.vi.index).all()
      self.sysinfo = sysinfo(self.vi.index)

def put_new_version(data):
  info = json.loads(data)
  def add_cluster_info():
    ci = Session.query(cluster_info).filter_by(uuid=info['uuid']).first()
    if ci is None:
      dt = datetime.strptime(info['cluster_creation_date'], "%Y-%m-%d %H:%M:%S.%f")
      ci = cluster_info(uuid=info['uuid'], 
                        organization=info['ownership']['organization'],
                        contact_email=info['ownership']['email'],
                        cluster_name=info['ownership']['name'],
                        description=info['ownership']['description'],
                        cluster_creation_date=dt,
                        num_versions=1)
      Session.add(ci)
      Session.commit()
    else:
      ci.num_versions += 1 

    return ci
 
  def add_version_info(ci):
    vi = version_info(cluster_id=ci.index, 
                      version_number=ci.num_versions,
                      version_date=datetime.now())
    Session.add(vi)
    return vi

  def add_components_info(vi):
    comps_count= info['components_count']
    comps_info = components_info(vid=vi.index,
                         num_data_bytes=comps_count['num_data_bytes'],
                         num_bytes_total=comps_count['num_bytes_total'],
                         num_osds=comps_count['num_osds'],
                         num_objects=comps_count['num_objects'],
                         num_pgs=comps_count['num_pgs'],
                         num_pools=comps_count['num_pools'],
                         num_mdss=comps_count['num_mdss'],
                         num_mons=comps_count['num_mons'])
    Session.add(comps_info)

  def add_crush_types(vi):
    for c in info['crush_types']:
      Session.add(crush_types(vid=vi.index, 
                            crush_type=c['type'],
                            crush_count=c['count']))

  def add_pools_info(vi):
    pools = info['pool_metadata']
    for p in pools:
      Session.add(pools_info(vid=vi.index,
                             pool_id=p['id'],
                             pool_type=p['type'],
                             pool_rep_size=p['size']))

  def add_sys_info(vi):
    si = info['sysinfo']
    while si:
      k,v = si.popitem()
      if k == 'os_info':
        for o in v:
          Session.add(os_info(vid=vi.index, 
                              os=o['os'],
                              count=o['count']))
      elif k == 'kernel_versions':
        for k in v:
          Session.add(kernel_versions(vid=vi.index,
                                      version=k['version'],
                                      count=k['count']))
      elif k == 'kernel_types':
        for k in v:
          Session.add(kernel_types(vid=vi.index,
                                   type=k['type'],
                                   count=k['count']))
      elif k == 'distros':
        for d in v:
          Session.add(distros(vid=vi.index,
                              distro=d['distro'],
                              count=d['count']))
      elif k == 'cpus':
        for c in v:
          Session.add(cpus(vid=vi.index,
                           cpu=c['cpu'],
                           count=c['count']))
      elif k == 'cpu_archs':
        for c in v:
          Session.add(cpu_archs(vid=vi.index,
                                arch=c['arch'],
                                count=c['count']))
      elif k == 'ceph_versions':
        for c in v:
          Session.add(ceph_versions(vid=vi.index,
                                    version=c['version'],
                                    count=c['count']))

  ci = add_cluster_info()
  add_version_info(ci)
  vi = Session.query(version_info).filter_by(cluster_id=ci.index, 
                                             version_number=ci.num_versions).first()
  add_components_info(vi)
  add_crush_types(vi)
  add_pools_info(vi)
  add_sys_info(vi)
 
def delete_uuid(uuid):
  ci = Session.query(cluster_info).filter_by(uuid=uuid).first()
  if ci is None:
    return {'status':400, 'msg':'No information for this UUID'}

  for v in Session.query(version_info).filter_by(cluster_id=ci.index).all():
    Session.query(components_info).filter_by(vid=v.index).delete()
    Session.query(crush_types).filter_by(vid=v.index).delete()
    Session.query(pools_info).filter_by(vid=v.index).delete()
    Session.query(os_info).filter_by(vid=v.index).delete()
    Session.query(kernel_versions).filter_by(vid=v.index).delete()
    Session.query(kernel_types).filter_by(vid=v.index).delete()
    Session.query(distros).filter_by(vid=v.index).delete()
    Session.query(cpus).filter_by(vid=v.index).delete()
    Session.query(cpu_archs).filter_by(vid=v.index).delete()
    Session.query(ceph_versions).filter_by(vid=v.index).delete()

    Session.flush()
    Session.delete(v)
    Session.flush()

  Session.delete(ci)
  return None

def get_uuids():
  return Session.query(cluster_info).all()

def get_versions(uuid):
  ci = Session.query(cluster_info).filter_by(uuid=uuid).first()
  if ci is None:
    return None

  return Session.query(version_info).filter_by(cluster_id=ci.index).all()

def get_brag(uuid, version_id):
  b = brag(uuid, version_id)
  if b.ci is None or b.vi is None:
    return None

  return b
