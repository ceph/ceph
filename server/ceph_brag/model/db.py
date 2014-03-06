import json
from datetime import datetime
import re
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
  num_bytes = Column(BigInteger)
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
  pool_type = Column(String(16))
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

def bytes_pretty_to_raw(pretty):
  mo = re.search("(\d+)\ (\S+)", pretty)
  if not mo:
    raise ValueError()

  byte_count = int(mo.group(1))
  byte_scale = mo.group(2)
  if byte_scale == 'kB':
    return byte_count >> 10
  if byte_scale == 'MB':
    return byte_count >> 20
  if byte_scale == 'GB':
    return byte_count >> 30
  if byte_scale == 'TB':
    return byte_count >> 40
  if byte_scale == 'PB':
    return byte_count >> 50
  if byte_scale == 'EB':
    return byte_count >> 60
  
  return byte_count

def bytes_raw_to_pretty(num_bytes):
  shift_limit = 100

  if num_bytes > shift_limit << 60:
    return str(num_bytes >> 60) + " EB"
  if num_bytes > shift_limit << 50:
    return str(num_bytes >> 50) + " PB"
  if num_bytes > shift_limit << 40:
    return str(num_bytes >> 40) + " TB"
  if num_bytes > shift_limit << 30:
    return str(num_bytes >> 30) + " GB"
  if num_bytes > shift_limit << 20:
    return str(num_bytes >> 20) + " MB"
  if num_bytes > shift_limit << 10:
    return str(num_bytes >> 10) + " kB"

  return str(num_bytes) + " bytes"

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
    nbytes = comps_count['num_bytes']
    comps_info = components_info(vid=vi.index,
                         num_bytes=bytes_pretty_to_raw(nbytes),
                         num_osds=comps_count['num_osds'],
                         num_objects=comps_count['num_objects'],
                         num_pgs=comps_count['num_pgs'],
                         num_pools=comps_count['num_pools'],
                         num_mdss=comps_count['num_mdss'],
                         num_mons=comps_count['num_mons'],
                         crush_types=','.join(info['crush_types']))
    Session.add(comps_info)

  def add_pools_info(vi):
    pools = info['pool_metadata']
    for p in pools:
      Session.add(pools_info(vid=vi.index,
                             pool_id=p['id'],
                             pool_type=p['type'],
                             pool_rep_size=p['size']))

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
