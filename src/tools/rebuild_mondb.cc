#include "auth/cephx/CephxKeyServer.h"
#include "common/errno.h"
#include "mon/AuthMonitor.h"
#include "mon/MonitorDBStore.h"
#include "os/ObjectStore.h"
#include "osd/OSD.h"

using namespace std;

static int update_auth(const string& keyring_path,
                       const OSDSuperblock& sb,
                       MonitorDBStore& ms);
static int update_monitor(const OSDSuperblock& sb, MonitorDBStore& ms);
static int update_osdmap(ObjectStore& fs,
                         OSDSuperblock& sb,
                         MonitorDBStore& ms);

int update_mon_db(ObjectStore& fs, OSDSuperblock& sb,
                  const string& keyring,
                  const string& store_path)
{
  MonitorDBStore ms(store_path);
  int r = ms.create_and_open(cerr);
  if (r < 0) {
    cerr << "unable to open mon store: " << store_path << std::endl;
    return r;
  }
  if ((r = update_auth(keyring, sb, ms)) < 0) {
    goto out;
  }
  if ((r = update_osdmap(fs, sb, ms)) < 0) {
    goto out;
  }
  if ((r = update_monitor(sb, ms)) < 0) {
    goto out;
  }
 out:
  ms.close();
  return r;
}

static void add_auth(KeyServerData::Incremental& auth_inc,
                     MonitorDBStore& ms)
{
  AuthMonitor::Incremental inc;
  inc.inc_type = AuthMonitor::AUTH_DATA;
  encode(auth_inc, inc.auth_data);
  inc.auth_type = CEPH_AUTH_CEPHX;

  bufferlist bl;
  __u8 v = 1;
  encode(v, bl);
  inc.encode(bl, CEPH_FEATURES_ALL);

  const string prefix("auth");
  auto last_committed = ms.get(prefix, "last_committed") + 1;
  auto t = make_shared<MonitorDBStore::Transaction>();
  t->put(prefix, last_committed, bl);
  t->put(prefix, "last_committed", last_committed);
  auto first_committed = ms.get(prefix, "first_committed");
  if (!first_committed) {
    t->put(prefix, "first_committed", last_committed);
  }
  ms.apply_transaction(t);
}

static int get_auth_inc(const string& keyring_path,
                        const OSDSuperblock& sb,
                        KeyServerData::Incremental* auth_inc)
{
  auth_inc->op = KeyServerData::AUTH_INC_ADD;

  // get the name
  EntityName entity;
  // assuming the entity name of OSD is "osd.<osd_id>"
  entity.set(CEPH_ENTITY_TYPE_OSD, std::to_string(sb.whoami));
  auth_inc->name = entity;

  // read keyring from disk
  KeyRing keyring;
  {
    bufferlist bl;
    string error;
    int r = bl.read_file(keyring_path.c_str(), &error);
    if (r < 0) {
      if (r == -ENOENT) {
        cout << "ignoring keyring (" << keyring_path << ")"
             << ": " << error << std::endl;
        return 0;
      } else {
        cerr << "unable to read keyring (" << keyring_path << ")"
             << ": " << error << std::endl;
        return r;
      }
    } else if (bl.length() == 0) {
      cout << "ignoring empty keyring: " << keyring_path << std::endl;
      return 0;
    }
    auto bp = bl.cbegin();
    try {
      decode(keyring, bp);
    } catch (const buffer::error& e) {
      cerr << "error decoding keyring: " << keyring_path << std::endl;
      return -EINVAL;
    }
  }

  // get the key
  EntityAuth new_inc;
  if (!keyring.get_auth(auth_inc->name, new_inc)) {
    cerr << "key for " << auth_inc->name << " not found in keyring: "
         << keyring_path << std::endl;
    return -EINVAL;
  }
  auth_inc->auth.key = new_inc.key;

  // get the caps
  map<string,bufferlist> caps;
  if (new_inc.caps.empty()) {
    // fallback to default caps for an OSD
    //   osd 'allow *' mon 'allow rwx'
    // as suggested by document.
    encode(string("allow *"), caps["osd"]);
    encode(string("allow rwx"), caps["mon"]);
  } else {
    caps = new_inc.caps;
  }
  auth_inc->auth.caps = caps;
  return 0;
}

// rebuild
//  - auth/${epoch}
//  - auth/first_committed
//  - auth/last_committed
static int update_auth(const string& keyring_path,
                       const OSDSuperblock& sb,
                       MonitorDBStore& ms)
{
  // stolen from AuthMonitor::prepare_command(), where prefix is "auth add"
  KeyServerData::Incremental auth_inc;
  int r;
  if ((r = get_auth_inc(keyring_path, sb, &auth_inc))) {
    return r;
  }
  add_auth(auth_inc, ms);
  return 0;
}

// stolen from Monitor::check_fsid()
static int check_fsid(const uuid_d& fsid, MonitorDBStore& ms)
{
  bufferlist bl;
  int r = ms.get("monitor", "cluster_uuid", bl);
  if (r == -ENOENT)
    return r;
  string uuid(bl.c_str(), bl.length());
  auto end = uuid.find_first_of('\n');
  if (end != uuid.npos) {
    uuid.resize(end);
  }
  uuid_d existing;
  if (!existing.parse(uuid.c_str())) {
    cerr << "error: unable to parse uuid" << std::endl;
    return -EINVAL;
  }
  if (fsid != existing) {
    cerr << "error: cluster_uuid " << existing << " != " << fsid << std::endl;
    return -EEXIST;
  }
  return 0;
}

// rebuild
//  - monitor/cluster_uuid
int update_monitor(const OSDSuperblock& sb, MonitorDBStore& ms)
{
  switch (check_fsid(sb.cluster_fsid, ms)) {
  case -ENOENT:
    break;
  case -EINVAL:
    return -EINVAL;
  case -EEXIST:
    return -EEXIST;
  case 0:
    return 0;
  default:
    ceph_abort();
  }
  string uuid = stringify(sb.cluster_fsid) + "\n";
  bufferlist bl;
  bl.append(uuid);
  auto t = make_shared<MonitorDBStore::Transaction>();
  t->put("monitor", "cluster_uuid", bl);
  ms.apply_transaction(t);
  return 0;
}

// rebuild
//  - osdmap/${epoch}
//  - osdmap/full_${epoch}
//  - osdmap/full_latest
//  - osdmap/first_committed
//  - osdmap/last_committed
int update_osdmap(ObjectStore& fs, OSDSuperblock& sb, MonitorDBStore& ms)
{
  const string prefix("osdmap");
  const string first_committed_name("first_committed");
  const string last_committed_name("last_committed");
  epoch_t first_committed = ms.get(prefix, first_committed_name);
  epoch_t last_committed = ms.get(prefix, last_committed_name);
  auto t = make_shared<MonitorDBStore::Transaction>();

  // trim stale maps
  unsigned ntrimmed = 0;
  // osdmap starts at 1. if we have a "0" first_committed, then there is nothing
  // to trim. and "1 osdmaps trimmed" in the output message is misleading. so
  // let's make it an exception.
  for (auto e = first_committed; first_committed && e < sb.get_oldest_map(); e++) {
    t->erase(prefix, e);
    t->erase(prefix, ms.combine_strings("full", e));
    ntrimmed++;
  }
  // make sure we have a non-zero first_committed. OSDMonitor relies on this.
  // because PaxosService::put_last_committed() set it to last_committed, if it
  // is zero. which breaks OSDMonitor::update_from_paxos(), in which we believe
  // that latest_full should always be greater than last_committed.
  if (first_committed == 0 && sb.get_oldest_map() < sb.get_newest_map()) {
    first_committed = 1;
  } else if (ntrimmed) {
    first_committed += ntrimmed;
  }
  if (first_committed) {
    t->put(prefix, first_committed_name, first_committed);
    ms.apply_transaction(t);
    t = make_shared<MonitorDBStore::Transaction>();
  }

  unsigned nadded = 0;

  auto ch = fs.open_collection(coll_t::meta());
  OSDMap osdmap;
  for (auto e = std::max(last_committed+1, sb.get_oldest_map());
       e <= sb.get_newest_map(); e++) {
    bool have_crc = false;
    uint32_t crc = -1;
    uint64_t features = 0;
    // add inc maps
    auto add_inc_result = [&] {
      const auto oid = OSD::get_inc_osdmap_pobject_name(e);
      bufferlist bl;
      int nread = fs.read(ch, oid, 0, 0, bl);
      if (nread <= 0) {
        cout << "missing " << oid << std::endl;
        return -ENOENT;
      }
      t->put(prefix, e, bl);

      OSDMap::Incremental inc;
      auto p = bl.cbegin();
      inc.decode(p);
      features = inc.encode_features | CEPH_FEATURE_RESERVED;
      if (osdmap.get_epoch() && e > 1) {
        if (osdmap.apply_incremental(inc)) {
          cerr << "bad fsid: "
               << osdmap.get_fsid() << " != " << inc.fsid << std::endl;
          return -EINVAL;
        }
        have_crc = inc.have_crc;
        if (inc.have_crc) {
          crc = inc.full_crc;
          bufferlist fbl;
          osdmap.encode(fbl, features);
          if (osdmap.get_crc() != inc.full_crc) {
            cerr << "mismatched inc crc: "
                 << osdmap.get_crc() << " != " << inc.full_crc << std::endl;
            return -EINVAL;
          }
          // inc.decode() verifies `inc_crc`, so it's been taken care of.
        }
      }
      return 0;
    }();
    switch (add_inc_result) {
    case -ENOENT:
      // no worries, we always have full map
      break;
    case -EINVAL:
      return -EINVAL;
    case 0:
      break;
    default:
      assert(0);
    }
    // add full maps
    {
      const auto oid = OSD::get_osdmap_pobject_name(e);
      bufferlist bl;
      int nread = fs.read(ch, oid, 0, 0, bl);
      if (nread <= 0) {
        cerr << "missing " << oid << std::endl;
        return -EINVAL;
      }
      t->put(prefix, ms.combine_strings("full", e), bl);

      auto p = bl.cbegin();
      osdmap.decode(p);
      if (osdmap.have_crc()) {
        if (have_crc && osdmap.get_crc() != crc) {
          cerr << "mismatched full/inc crc: "
               << osdmap.get_crc() << " != " << crc << std::endl;
          return -EINVAL;
        }
        uint32_t saved_crc = osdmap.get_crc();
        bufferlist fbl;
        osdmap.encode(fbl, features);
        if (osdmap.get_crc() != saved_crc) {
          cerr << "mismatched full crc: "
               << saved_crc << " != " << osdmap.get_crc() << std::endl;
          return -EINVAL;
        }
      }
    }
    nadded++;

    // last_committed
    t->put(prefix, last_committed_name, e);
    // full last
    t->put(prefix, ms.combine_strings("full", "latest"), e);

    // this number comes from the default value of osd_target_transaction_size,
    // so we won't OOM or stuff too many maps in a single transaction if OSD is
    // keeping a large series of osdmap
    static constexpr unsigned TRANSACTION_SIZE = 30;
    if (t->size() >= TRANSACTION_SIZE) {
      ms.apply_transaction(t);
      t = make_shared<MonitorDBStore::Transaction>();
    }
  }
  if (!t->empty()) {
    ms.apply_transaction(t);
  }
  t.reset();

  string osd_name("osd.");
  osd_name += std::to_string(sb.whoami);
  cout << std::left << setw(8)
       << osd_name << ": "
       << ntrimmed << " osdmaps trimmed, "
       << nadded << " osdmaps added." << std::endl;
  return 0;
}

