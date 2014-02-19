import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
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
  byte_count = Column(Integer)
  byte_scale = Column(String(8))
  num_osds = Column(Integer)
  num_objects = Column(Integer)
  num_pgs = Column(Integer)
  num_pools = Column(Integer)
  num_mdss = Column(Integer)
  num_mons = Column(Integer)
  crush_types = Column(String(256))

class pools_info(Base):
  __tablename__ = 'pools_info'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  pool_id = Column(String(8))
  pool_name = Column(String(16))
  pool_rep_size = Column(Integer)

class osds_info(Base):
  __tablename__ = 'osds_info'

  index = Column(Integer, primary_key=True)
  vid = Column(ForeignKey('version_info.index'))
  osd_id = Column(String(8))
  nw_address = Column(String(16))
  hostname = Column(String(16))
  swap_kb = Column(Integer)
  mem_kb = Column(Integer)
  arch = Column(String(16))
  cpu = Column(String(16))
  os = Column(String(16))
  os_version = Column(String(16))
  os_desc = Column(String(64))
  distro = Column(String(64))
  ceph_version = Column(String(64))

class brag(object):
  def __init__(self, uuid, version_number):
    self.ci = Session.query(cluster_info).filter_by(uuid=uuid).first()
    if self.ci is not None:
      self.vi = Session.query(version_info).filter_by(cluster_id=self.ci.index, version_number=version_number).first()
    
    if self.ci is not None and self.vi is not None:
      self.comps = Session.query(components_info).filter_by(vid=self.vi.index).first()
      self.pools = Session.query(pools_info).filter_by(vid=self.vi.index).all()
      self.osds = Session.query(osds_info).filter_by(vid=self.vi.index).all()

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
                         byte_count=comps_count['bytes']['count'],
                         byte_scale=comps_count['bytes']['scale'],
                         num_osds=comps_count['osds'],
                         num_objects=comps_count['objects'],
                         num_pgs=comps_count['pgs'],
                         num_pools=comps_count['pools'],
                         num_mdss=comps_count['mdss'],
                         num_mons=comps_count['mons'],
                         crush_types=','.join(info['crush_types']))
    Session.add(comps_info)

  def add_pools_info(vi):
    pools = info['pool_metadata']
    for p in pools:
      Session.add(pools_info(vid=vi.index,
                             pool_id=p['id'],
                             pool_name=p['name'],
                             pool_rep_size=p['rep_size']))

  def add_osds_info(vi):
    osds = info['sysinfo']
    for o in osds:
      osd = osds_info(vid=vi.index,
                      osd_id=o['id'],
                      nw_address=o['nw_info']['address'],
                      hostname=o['nw_info']['hostname'],
                      swap_kb=o['hw_info']['swap_kb'],
                      mem_kb=o['hw_info']['mem_kb'],
                      arch=o['hw_info']['arch'],
                      cpu=o['hw_info']['cpu'],
                      os=o['os_info']['os'],
                      os_version=o['os_info']['version'],
                      os_desc=o['os_info']['description'],
                      distro=o['os_info']['distro'],
                      ceph_version=o['ceph_version'])
      Session.add(osd)
                    
  ci = add_cluster_info()
  add_version_info(ci)
  vi = Session.query(version_info).filter_by(cluster_id=ci.index, 
                                             version_number=ci.num_versions).first()
  add_components_info(vi)
  add_pools_info(vi)
  add_osds_info(vi)
 
def delete_uuid(uuid):
  ci = Session.query(cluster_info).filter_by(uuid=uuid).first()
  if ci is None:
    return {'status':400, 'msg':'No information for this UUID'}

  for v in Session.query(version_info).filter_by(cluster_id=ci.index).all():
    Session.query(components_info).filter_by(vid=v.index).delete()
    Session.query(pools_info).filter_by(vid=v.index).delete()
    Session.query(osds_info).filter_by(vid=v.index).delete()
    Session.delete(v)

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
