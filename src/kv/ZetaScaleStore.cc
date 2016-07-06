// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * ZetaScale storage engine key-value backend for Ceph.
 * Author: Evgeniy Firsov <evgeniy.firsov@sandisk.com>
 *
 */

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <map>
#include <memory>
#include <set>
#include <string>

using std::string;

#include "zs/api/zs.h"

#include "ZetaScaleStore.h"

#include "include/str_list.h"
#include "include/str_map.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/hex.h"
#include "common/perf_counters.h"

#include "os/bluestore/kv.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_zs
#undef dout_prefix
#define dout_prefix *_dout << "zs: "
#define LOG_PREFIX << __func__ << ":" << __LINE__ << " "
#define dtrace dout(30) LOG_PREFIX
#define dwarn dout(0)
#define dinfo dout(0)

static std::string decode_key(const std::string &k)
{
  char buf[256];

  assert(k.length() * 2 + 1 <= sizeof(buf));

  int pos = 0;
  for (unsigned int i = 0; i < k.length(); i++) {
    if (k[i] == '_' || (k[i] >= 'a' && k[i] <= 'z') ||
	(k[i] >= 32 && k[i] <= 'Z'))
      buf[pos++] = k[i];
    else
      pos += sprintf(&buf[pos], "%.2x", (int)(unsigned char)k[i]);
  }

  return buf;
}

std::string delete_logging_prefix(const std::string &key)
{
  size_t pos = key.find_first_of('_', 2);
  return key.substr(pos + 1);
}

static void append_logging_prefix(const std::string &key, std::string &p)
{
  p.clear();

  p.append(key.length() == 40 ? "1_" : "2_");

  uint64_t omap_id;
  _key_decode_u64(key.c_str(), &omap_id);
  std::ostringstream os;
  os << omap_id << "_";

  p.append(os.str());

  dtrace << " " << decode_key(p) << "(" << p.length() << ")" << dendl;
}

static bool is_logging_prefixed(const std::string &key)
{
  return (key[0] == '1' || key[0] == '2') && key[1] == '_';
}

static bool is_logging(const std::string &prefix, const std::string &key)
{
  if (prefix[0] != 'M' ||
      (key.length() != 40 &&
       strncmp(key.c_str() + 8, "._info", sizeof("._info")) &&
       strncmp(key.c_str() + 8, "._fastinfo", sizeof("._fastinfo"))))
    return false;

  dtrace << " " << prefix << " " << decode_key(key) << "(" << key.length()
	 << ")" << dendl;

  return true;
}

static bool is_pglog(const std::string &prefix, const std::string &key)
{
  return prefix[0] == 'M' && key.length() == 40;
}

inline std::string combine_strings(const std::string &prefix,
				   const std::string &value, bool log)
{
  std::string out;

  if (log && is_logging(prefix, value))
    append_logging_prefix(value, out);

  out.append(prefix.begin(), prefix.end());
  out.push_back(0);
  out.append(value.begin(), value.end());
  return out;
}

static void append_chunk_number(std::string &key, unsigned int key_len,
				unsigned int j)
{
  key.resize(key_len + 4);
  key[key_len] = 0;
  key[key_len + 1] = '0' + j / 10;
  key[key_len + 2] = '0' + j % 10;
  key[key_len + 3] = 0;
}

// One instance of ZetaScale per process
static __thread struct ZS_thread_state *zs_thrd_state = NULL;
static struct ZS_state *zs_state = NULL;

struct ZS_thread_state *_thd_state()
{
  if (zs_thrd_state)
    return zs_thrd_state;

  ZS_status_t status = ZSInitPerThreadState(zs_state, &zs_thrd_state);
  if (status != ZS_SUCCESS) {
    derr << "ZSInitPerThreadState failed: " << ZSStrError(status) << dendl;
    zs_thrd_state = NULL;
  }

  return zs_thrd_state;
}

void ZSStore::SetPropertiesFromString(const string &opt_str)
{
  map<string, string> str_map;
  (void)get_str_map(opt_str, &str_map, ",\n;");
  for (const auto &it : str_map) {
    ZSSetProperty(it.first.c_str(), it.second.c_str());
    dinfo << " set zs option " << it.first << " = " << it.second << dendl;
  }
}

int ZSStore::_init(bool format)
{
  char str_buf[32];

  if (zs_state)
    return 0;

  if (getenv("ZS_LIB"))
    dinfo << "ZS library search path set to: " << getenv("ZS_LIB") << dendl;

  std::string path = g_conf->osd_data + "/block.db";
  ZSSetProperty("ZS_FLASH_FILENAME", path.c_str());

  int fd = ::open(path.c_str(), O_RDONLY);
  off_t size = ::lseek(fd, 0, SEEK_END);
  ::close(fd);
  if(size == (off_t)-1) {
    derr << path << " failed to get" << path << " size:"
      << strerror(errno) << dendl;
    return 1;
  }

  if (size < ZS_MINIMUM_DB_SIZE) {
    derr << "Given ZS data partition size " << size
	 << " is less then minimum 64GiB" << dendl;
    dinfo << "Please adjust partition size or bluestore_block_db_size"
	     "accordingly"
	  << dendl;
    return 1;
  }

  uint64_t flash_size = size / (1024 * 1024 * 1024);
  dinfo << "ZS data partition size is set to " << flash_size << "Gb" << dendl;

  sprintf(str_buf, "%lu", flash_size);
  ZSSetProperty("ZS_FLASH_SIZE", str_buf);
  ZSSetProperty("ZS_BLOCK_SIZE", "8192");
  ZSSetProperty("ZS_CACHE_SIZE", "134217728");
  ZSSetProperty("ZS_BTREE_L1CACHE_SIZE", default_cache_size);
  ZSSetProperty("ZS_STORM_MODE", "0");
  ZSSetProperty("ZS_LOG_LEVEL", "info");
  ZSSetProperty("ZS_COMPRESSION", "0");
  ZSSetProperty("ZS_STATS_DUMP_INTERVAL", "60");
  ZSSetProperty("ZS_STATS_LEVEL", "5");
  ZSSetProperty("ZS_STATS_FILE", (g_conf->log_file + ".zs_stats").c_str());

  ZSSetProperty("ZS_FLOG_MODE", "ZS_FLOG_NVRAM_MODE");
  ZSSetProperty("ZS_NVR_OFFSET", "0");
  ZSSetProperty("ZS_LOG_BLOCK_SIZE", "512");

  path = g_conf->osd_data + "/block.wal";
  ZSSetProperty("ZS_FLOG_NVRAM_FILE", path.c_str());
  ZSSetProperty("ZS_NVR_FILENAME", path.c_str());
  ZSSetProperty("ZS_NVR_HW_DURABLE", "0");

  fd = ::open(path.c_str(), O_RDONLY);
  size = ::lseek(fd, 0, SEEK_END);
  ::close(fd);
  if(size == (off_t)-1) {
    derr << path << " failed to get" << path << " size:"
      << strerror(errno) << dendl;
    return 1;
  }

  if (size > (512 * 1024 * 1024)) {
    dinfo << "Adjusting ZS log partition to maximum supported size." << dendl;
    size = 512 * 1024 * 1024;
  }

  dinfo << "ZS log partition size is set to " << size << dendl;

  sprintf(str_buf, "%lu", size / 2);
  ZSSetProperty("ZS_FLOG_NVRAM_FILE_OFFSET", str_buf);
  ZSSetProperty("ZS_NVR_LENGTH", str_buf);

  // ZS_NVR_LENGTH/ZS_NVR_PARTITIONS/ZS_MAX_NUM_LC/ZS_NVR_OBJECT_SIZE
  int num_lc = (size / 2) / 65536;

  dinfo << "ZS log partition size is set to " << size << dendl;

  sprintf(str_buf, "%u", num_lc);

  ZSSetProperty("ZS_MAX_NUM_LC", str_buf);

  ZSSetProperty("ZS_ASYNC_PUT_THREADS", "3");
  ZSSetProperty("ZS_MAX_NUM_CONTAINERS", "8");
  ZSSetProperty("ZS_SCAVENGER_THREADS", "8");

  ZSSetProperty("ZS_CRASH_DIR", g_conf->osd_data.c_str());

  ZSSetProperty("ZS_LOG_FILE", (g_conf->log_file + ".zs").c_str());

  ZSLoadProperties(getenv("ZS_PROPERTY_FILE"));

  if (options.length())
    SetPropertiesFromString(options);

  ZSSetProperty("ZS_REFORMAT", format ? "1" : "0");
  if (format)
    dinfo << "Reformatting ZS storage" << dendl;

  assert(!zs_state);

  dtrace << "ZSInit" << dendl;

  ZS_status_t status = ZSInit(&zs_state);
  if (status != ZS_SUCCESS) {
    derr << "ZSInit failed with error" << ZSStrError(status) << dendl;
    zs_state = NULL;
  }

  return status == ZS_SUCCESS ? 0 : 1;
}

static bool open_or_create_container(int create, bool log, ZS_cguid_t *cguid,
				     const char *name)
{
  ZS_container_props_t props;

  ZSLoadCntrPropDefaults(&props);

  dtrace << "ZSOpenContainer: "
	 << " " << create << " name: " << name << dendl;

  props.size_kb = 0;

  if (log)
    props.flags = ZS_LOG_CTNR;

  ZS_status_t status =
      ZSOpenContainer(_thd_state(), name, &props, ZS_CTNR_RW_MODE, cguid);

  if (create && (status == ZS_SUCCESS || status == ZS_CONTAINER_UNKNOWN)) {
    if (status == ZS_SUCCESS)
      (void)ZSDeleteContainer(_thd_state(), *cguid);

    status = ZSOpenContainer(_thd_state(), name, &props,
			     ZS_CTNR_RW_MODE | create, cguid);
  }

  if (status != ZS_SUCCESS)
    derr << "failed. name: " << name << " error: " << ZSStrError(status)
	 << dendl;

  return status == ZS_SUCCESS;
}

int ZSStore::do_open(ostream &out, int create)
{
  _init(create == ZS_CTNR_CREATE);

  if (!open_or_create_container(create, false, &cguid, CEPH_ZS_CONTAINER_NAME))
    return -1;

  if (!open_or_create_container(create, true, &cguid_lc,
				CEPH_ZS_LOG_CONTAINER_NAME))
    return -1;

  const char *log_device = ZSGetProperty(
      "ZS_FLOG_NVRAM_FILE", g_conf->bluestore_block_wal_path.c_str());
  if (log_device) {
    dev_log_fd = ::open(log_device, O_RDWR);
    if (dev_log_fd <= 0)
      derr << "open failed" << strerror(errno) << dendl;
    dtrace << "do_open dev_log_fd=" << dev_log_fd << " name: " << log_device
	   << dendl;
    assert(dev_log_fd >= 0);
  }

  dev_data_fd = ::open(ZSGetProperty("ZS_FLASH_FILENAME",
				     g_conf->bluestore_block_db_path.c_str()),
		       O_RDWR);
  if (dev_data_fd <= 0)
    derr << "open data failed" << strerror(errno) << dendl;
  dtrace << "do_open dev_data_fd=" << dev_data_fd << " data_path="
	 << ZSGetProperty("ZS_FLASH_FILENAME",
			  g_conf->bluestore_block_db_path.c_str())
	 << dendl;
  assert(dev_data_fd >= 0);

  return 0;
}

void ZSStore::close()
{
  ZS_status_t status = ZSCloseContainer(_thd_state(), cguid);
  if (status != ZS_SUCCESS)
    derr << "ZSCloseContainer failed: " << ZSStrError(status) << dendl;
  status = ZSCloseContainer(_thd_state(), cguid_lc);
  if (status != ZS_SUCCESS)
    derr << "ZSCloseContainer failed: " << ZSStrError(status) << dendl;
}

ZSStore::~ZSStore() { close(); }
int ZSStore::submit_transaction(KeyValueDB::Transaction t)
{
  ZSTransactionImpl *zt = static_cast<ZSTransactionImpl *>(t.get());

  dtrace << " " << zt->get_ops().size() << dendl;

  for (auto &op : zt->get_ops()) {
    if (op.first == ZSTransactionImpl::WRITE) {
      delete_ops.erase(op.second.first);
      write_ops[op.second.first] = op.second.second;
    } else if (op.first == ZSTransactionImpl::MERGE) {
      delete_ops.erase(op.second.first);
      write_ops[op.second.first] =
	  _merge(op.second.first, write_ops[op.second.first], op.second.second);
    } else
      delete_ops.insert(op.second.first);
  }

  return 0;
}

int ZSStore::_submit_transaction_sync(KeyValueDB::Transaction tsync)
{
  submit_transaction(tsync);

  if (transaction_start())
    return -1;

  if (transaction_submit())
    return -1;

  if (transaction_commit())
    return -1;

  return 0;
}

int ZSStore::submit_transaction_sync(KeyValueDB::Transaction tsync)
{
  dtrace << " writes=" << write_ops.size() << " del=" << delete_ops.size()
	 << dendl;

  int r = _submit_transaction_sync(tsync);

  write_ops.clear();
  delete_ops.clear();

  return r;
}

int ZSStore::transaction_submit()
{
  if (write_ops.size() && _batch_set(write_ops))
    return -1;

  for (auto &key : delete_ops)
    if (_rmkey(key))
      return -1;

  return 0;
}

int ZSStore::transaction_start()
{
  ZS_status_t status = ZSTransactionStart(_thd_state());

  dtrace << "ZSTransactionStart: " << cguid << dendl;

  if (status != ZS_SUCCESS) {
    derr << "ZSTransactionStart failed: " << ZSStrError(status) << dendl;
    return -1;
  }

  return 0;
}

int ZSStore::transaction_commit()
{
  int r = fdatasync(dev_data_fd);
  if (r < 0)
    derr << "fdatasync failed fd=" << dev_data_fd << " :" << strerror(errno)
	 << dendl;

  ZS_status_t status = ZSTransactionCommit(_thd_state());
  if (status != ZS_SUCCESS)
    derr << "ZSTransactionCommit failed: " << ZSStrError(status) << dendl;

  dtrace << "ZSTransactionCommit: " << cguid << dendl;

  if (dev_log_fd >= 0) {
    r = fdatasync(dev_log_fd);
    if (r < 0)
      derr << "fdatasync failed fd=" << dev_log_fd << " :" << strerror(errno)
	   << dendl;
  }

  return status == ZS_SUCCESS ? 0 : -1;
}

int ZSStore::transaction_rollback()
{
  ZS_status_t status = ZSTransactionCommit(_thd_state());

  if (status != ZS_SUCCESS)
    derr << "ZSTransactionCommit failed: " << ZSStrError(status) << dendl;

  return status == ZS_SUCCESS ? 0 : -1;
}

bool ZSStore::key_is_prefixed(const string &prefix, const string &full_key)
{
  if ((full_key.length() > prefix.length()) &&
      (full_key.c_str()[prefix.length()] == '\0'))
    return !memcmp(full_key.c_str(), prefix.c_str(), prefix.length());

  return false;
}

void ZSStore::ZSTransactionImpl::set(const string &prefix, const string &k,
				     const bufferlist &to_set_bl)
{
  dtrace << " " << prefix << " " << decode_key(k) << dendl;

  ops.push_back(
      make_pair(WRITE, make_pair(combine_strings(prefix, k, true), to_set_bl)));
}

void ZSStore::ZSTransactionImpl::rmkey(const string &prefix, const string &k)
{
  dtrace << " " << prefix << " " << decode_key(k) << " " << is_pglog(prefix, k)
	 << " " << k.length() << dendl;

  if (is_pglog(prefix, k))
    pg_log_key = combine_strings(prefix, k, true);
  else
    ops.push_back(make_pair(
	DELETE, make_pair(combine_strings(prefix, k, true), bufferlist())));
}

void ZSStore::ZSTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first(); it->valid(); it->next()) rmkey(prefix, it->key());
}

bufferlist ZSStore::_merge(const std::string &key, const bufferlist &_base,
			   const bufferlist &value)
{
  bufferlist rbase;
  std::string out;

  if (!_base.length()) {
    int r = _get(key, &rbase);
    assert(!r || r == -ENOENT);
  }

  /* Use local, read from disk, value, when argument _base is empty
   * Make _base contiguous if it's not */
  const bufferlist &base =
      !_base.length() ? rbase
		      : (_base.is_contiguous() ? _base : bufferlist(_base));

  /* Make "value" contiguous, so we can use c_str() of front buffer later */
  const bufferlist &v =
      value.is_contiguous() && value.length() > 0 ? value : bufferlist(value);

  dtrace << " " << key << " " << base.length() << " " << value.length() << " "
       << dendl;

  for (auto &p : merge_ops) {
    if (!key_is_prefixed(p.first, key))
      continue;

    if (base.length())
      p.second->merge(base.front().c_str(), base.length(),
		      v.buffers().front().c_str(), v.length(), &out);
    else
      p.second->merge_nonexistent(v.buffers().front().c_str(), v.length(),
				  &out);
  }

  bufferlist res;
  res.append(bufferptr(out.c_str(), out.length()));
  return res;
}

void ZSStore::ZSTransactionImpl::merge(const std::string &prefix,
				       const std::string &k,
				       const bufferlist &value)
{
  ops.push_back(
      make_pair(MERGE, make_pair(combine_strings(prefix, k, true), value)));
}

#define WRITE_BATCH_SIZE 256

static bool enqueue_obj(ZS_cguid_t cguid, ZS_obj_t *objs, uint32_t *i,
			const char *key, uint32_t key_len, char *data,
			uint64_t data_len)
{
  uint32_t objs_written, _i = *i;

  objs[_i].flags = 0;
  objs[_i].key = (char *)key;
  objs[_i].key_len = key_len;
  objs[_i].data = data;
  objs[_i].data_len = data_len;

  *i = (*i + 1) % WRITE_BATCH_SIZE;
  if (!*i) {
    ZS_status_t status =
	ZSMPut(_thd_state(), cguid, WRITE_BATCH_SIZE, objs, 0, &objs_written);
    if (status != ZS_SUCCESS)
      return false;
    assert(objs_written == WRITE_BATCH_SIZE);
  }

  return true;
}

int ZSStore::_batch_set(const ZSStore::ZSMultiMap &ops)
{
#define CHUNK_SIZE 1024
  char *key;
  std::vector<std::string> keys;
  ZS_obj_t objs[WRITE_BATCH_SIZE], objs_lc[WRITE_BATCH_SIZE];
  ZS_status_t status = ZS_SUCCESS;
  uint32_t i = 0, k = 0, l1 = 0, objs_written;

  dtrace << "ZSMPut count=" << ops.size() << dendl;

  for (auto &op : ops) {
    int key_len = op.first.length();
    key = (char *)op.first.c_str();
    bufferlist *o = (bufferlist *)&op.second;
    char *ptr = (char *)o->c_str();
    unsigned int length = op.second.length();

    dtrace << " i=" << i << " l1=" << l1 << " " << decode_key(op.first) << "("
	   << op.first.length() << ")"
	   << " data_size=" << length << dendl;

    bool lc = is_logging_prefixed(op.first);

    if (lc) {
      if (!enqueue_obj(cguid_lc, objs_lc, &l1, op.first.c_str(),
		       op.first.length(), ptr, length))
	return -1;
      continue;
    }

    if (length > 4096) {
      if (!enqueue_obj(cguid, objs, &i, op.first.c_str(), op.first.length(),
		       ptr, length))
	return -1;
    } else {
      for (unsigned int j = 0;
	   j < (length + CHUNK_SIZE - 1) / CHUNK_SIZE && j < MAX_CHUNK_COUNT;
	   j++) {
	if (j) {
	  keys.resize(k + 1);
	  keys[k] = op.first;
	  append_chunk_number(keys[k], key_len, j);
	  dtrace << keys[k] << dendl;
	  key = (char *)keys[k].c_str();
	  objs[i].key_len = key_len + 3;
	  k++;
	} else
	  objs[i].key_len = key_len;

	if (!enqueue_obj(cguid, objs, &i, key, objs[i].key_len,
			 ptr + j * CHUNK_SIZE,
			 (length - j * CHUNK_SIZE > CHUNK_SIZE &&
			  j < MAX_CHUNK_COUNT - 1)
			     ? CHUNK_SIZE
			     : length - j * CHUNK_SIZE))
	  return -1;

	if (!i) {
	  k = 0;
	  keys.clear();
	}
      }
    }
  }

  if (i) {
    status = ZSMPut(_thd_state(), cguid, i, objs, 0, &objs_written);
    assert(status != ZS_SUCCESS || objs_written == i);
  }

  if (l1) {
    status = ZSMPut(_thd_state(), cguid_lc, l1, objs_lc, 0, &objs_written);
    assert(status != ZS_SUCCESS || objs_written == l1);
  }

  dtrace << "ZSMPut flush=" << i << " l1=" << l1
	 << " status=" << ZSStrError(status) << dendl;

  if (status != ZS_SUCCESS)
    derr << "ZSMPut flush=" << ZSStrError(status) << dendl;

  return status == ZS_SUCCESS ? 0 : -1;
}

int ZSStore::_rmkey(const std::string &_key)
{
  ZS_status_t status;

  if (is_logging_prefixed(_key)) {
    dtrace << "ZSDeleteObject(lcrm): [" << decode_key(_key) << "]" << dendl;
    status = ZSWriteObject(_thd_state(), cguid_lc, _key.c_str(), _key.length(),
			   "", 1, ZS_WRITE_TRIM);
  } else {
    std::string key(_key);
    int key_len = key.length();

    for (unsigned int i = 0; i < MAX_CHUNK_COUNT; i++) {
      dtrace << "ZSDeleteObject: [" << decode_key(key) << "]" << i << dendl;

      if (i)
	append_chunk_number(key, key_len, i);

      status = ZSDeleteObject(_thd_state(), cguid, key.c_str(), key.length());
      if (status == ZS_OBJECT_UNKNOWN)
	break;
    }
  }

  return (status == ZS_SUCCESS || status == ZS_OBJECT_UNKNOWN) ? 0 : -1;
}

int ZSStore::_get(const string &key, bufferlist *out)
{
  int n_out;

  out->clear();

  dtrace << "ZSReadObject: [" << decode_key(key) << "]" << dendl;

  if (is_logging_prefixed(key)) {
    uint64_t datalen = 0;
    char *dataw = NULL;
    ZS_status_t status = ZSReadObject(_thd_state(), cguid_lc, key.c_str(),
				      key.length(), &dataw, &datalen);
    dtrace << "ZSReadObject logging: [" << status << "]" << datalen << dendl;
    if (status == ZS_SUCCESS)
      out->push_back(buffer::claim_malloc(datalen, dataw));

    return status == ZS_SUCCESS ? 0 : -EIO;
  }

  string key_end;
  ZS_range_meta_t meta;
  ZS_range_data_t values[MAX_CHUNK_COUNT];
  struct ZS_cursor *cursor;

  key_end.append(key);

  memset(&meta, 0, sizeof(meta));

  append_chunk_number(key_end, key_end.length(), MAX_CHUNK_COUNT);

  meta.key_start = (char *)key.c_str();
  meta.keylen_start = key.length();
  meta.key_end = (char *)key_end.c_str();
  meta.keylen_end = key_end.length();
  meta.flags = (ZS_range_enums_t)(ZS_RANGE_START_GE | ZS_RANGE_END_LE);

  ZS_status_t status =
      ZSGetRange(_thd_state(), cguid, ZS_RANGE_PRIMARY_INDEX, &cursor, &meta);
  if (status != ZS_SUCCESS)
    return -EIO;

  status =
      ZSGetNextRange(_thd_state(), cursor, MAX_CHUNK_COUNT, &n_out, values);

  dtrace << " key_start=" << decode_key(key) << "(" << key.length()
	 << ") key_end=" << decode_key(key_end) << "(" << key_end.length()
	 << ") status=" <<  status << " n_out=" << n_out << dendl;

  if (status == ZS_SUCCESS) {
    for (int i = 0; i < n_out; i++)
      out->push_back(buffer::claim_malloc(values[i].datalen, values[i].data));
  } else if (status != ZS_QUERY_DONE)
    derr << "ZSReadObject failed: " << ZSStrError(status) << dendl;

  ZS_status_t status1 = ZSGetRangeFinish(_thd_state(), cursor);
  if (status1 != ZS_SUCCESS)
    derr << "ZSGetRangeFinish failed: " << ZSStrError(status1) << dendl;

  if (status == ZS_SUCCESS)
    return 0;
  else if (status == ZS_OBJECT_UNKNOWN || (status == ZS_QUERY_DONE && !n_out))
    return -ENOENT;

  return -EIO;
}

int ZSStore::get(const string &prefix, const std::set<string> &keys,
		 std::map<string, bufferlist> *out)
{
  for (const auto &key : keys) {
    bufferlist bl;
    if (!_get(combine_strings(prefix, key, true), &bl))
      out->insert(make_pair(key, bl));
  }

  return 0;
}

int ZSStore::ZSWholeSpaceIteratorImpl::split_key(const char *pkey,
						 uint32_t pkey_len,
						 string *prefix, string *key)
{
  char *separator = (char *)memchr(pkey, 0, pkey_len);
  if (separator == NULL)
    return -EINVAL;

  size_t prefix_len = size_t(separator - pkey);
  if (prefix_len >= pkey_len)
    return -EINVAL;

  if (prefix)
    *prefix = string(pkey, prefix_len);

  if (key)
    *key = string(separator + 1, pkey_len - prefix_len - 1);

  return 0;
}

void ZSStore::ZSWholeSpaceIteratorImpl::finish()
{
  dtrace << " " << valid() << " " << dendl;

  invalidate();

  if (cursor) {
    ZS_status_t status = ZSGetRangeFinish(_thd_state(), cursor);
    if (status != ZS_SUCCESS)
      derr << "ZSGetRangeFinish failed: " << ZSStrError(status) << dendl;

    cursor = NULL;
  }

  if (lc_it) {
    dtrace << "finish logging container enumeration" << dendl;
    ZS_status_t status = ZSFinishEnumeration(_thd_state(), lc_it);
    if (status != ZS_SUCCESS)
      derr << "ZSFinishEnumeration failed: " << ZSStrError(status) << dendl;

    lc_it = NULL;
  }
}

int ZSStore::ZSWholeSpaceIteratorImpl::seek(const char *key, uint32_t length,
					    bool _direction, bool inclusive)
{
  ZS_status_t status;
  std::string skey(key, length);
  const char *smallest = "";

  direction = _direction;

  finish();

  if (length == 11 && key[0] == 'M' && key[10] == '-') {
    char buf[32];
    uint64_t omap_id;
    _key_decode_u64(key + 2, &omap_id);
    int len = sprintf(buf, "2_%ld", omap_id);

    status = ZSEnumeratePGObjects(_thd_state(), cguid_lc, &lc_it, buf, len);
    if (status != ZS_SUCCESS)
      derr << "ZSEnumeratePGObjects failed: " << ZSStrError(status) << dendl;

    dtrace << "start logging container enumeration" << dendl;
  }

  memset(&meta, 0, sizeof(meta));

  meta.key_start = (char *)key;
  meta.keylen_start = length;

  if (key) {
    if (direction)
      meta.flags =
	  (ZS_range_enums_t)(inclusive ? ZS_RANGE_START_GE : ZS_RANGE_START_GT);
    else {
      meta.flags =
	  (ZS_range_enums_t)(inclusive ? ZS_RANGE_START_LE : ZS_RANGE_START_LT);
      if (inclusive)
	append_chunk_number(skey, skey.length(), MAX_CHUNK_COUNT);
      meta.key_start = (char *)skey.c_str();
      meta.keylen_start = skey.length();
    }
  } else if (!direction) {
    /* Handle descending unbounded query, used for seek_to_last() */
    meta.flags = (ZS_range_enums_t)ZS_RANGE_END_GE;
    meta.key_end = (char *)smallest;
    meta.keylen_end = 0;
  }

  if (snap_seqno != 0) {
    meta.flags = (ZS_range_enums_t)(meta.flags | ZS_RANGE_SEQNO_LE);
    meta.end_seq = snap_seqno;
  }

  status =
      ZSGetRange(_thd_state(), cguid, ZS_RANGE_PRIMARY_INDEX, &cursor, &meta);

  dtrace << " " << cguid << " " << inclusive << " " << direction << " ["
	 << decode_key(key ? key : smallest) << "] " << snap_seqno
	 << " length: " << length << dendl;

  if (status != ZS_SUCCESS) {
    derr << "ZSGetRange failed: " << ZSStrError(status) << dendl;
    assert(!cursor);
  } else if (_next())
    return -1;

  return (lc_it != NULL || status == ZS_SUCCESS) ? 0 : -1;
}

int ZSStore::ZSWholeSpaceIteratorImpl::_next()
{
  ZS_status_t status;
  int n_out;

  cur_key.clear();

  if (lc_it != NULL) {
    char *ekey = NULL, *data = NULL;
    uint32_t keylen;
    uint64_t datalen;

    status = ZSNextEnumeratedObject(_thd_state(), lc_it, &ekey, &keylen, &data,
				    &datalen);
    if (status == ZS_SUCCESS) {
      char *p = strchr(ekey + 2, '_') + 1;
      cur_key.append(p, keylen - (p - ekey));
      value_bl.clear();
      value_bl.append(buffer::claim_malloc(datalen, data));
      free(ekey);
      dtrace << " lc key=" << decode_key(cur_key) << dendl;
      return 0;
    }

    dtrace << "finish logging container enumeration" << dendl;

    status = ZSFinishEnumeration(_thd_state(), lc_it);
    if (status != ZS_SUCCESS)
      derr << "ZSFinishEnumeration failed: " << ZSStrError(status) << dendl;

    lc_it = NULL;
  }

  if (!cursor)
    return -1;

  status = ZSGetNextRange(_thd_state(), cursor, MAX_CHUNK_COUNT - count, &n_out,
			  values + count);
  n_out += count;

  bool _valid = (status == ZS_SUCCESS || status == ZS_QUERY_DONE) && n_out > 0;

  dtrace << _valid << " n_out " << n_out << " count=" << count << dendl;

  if (n_out) {
    std::string key(values[0].key, values[0].keylen);
    cur_key.assign(values[0].key, values[0].keylen);

    value_bl.clear();
    value_bl.append(buffer::claim_malloc(values[0].datalen, values[0].data));

    free(values[0].key);

    dtrace << "first key=" << decode_key(cur_key) << dendl;

    key += '\0';
    int i = 1;
    while (i < n_out && values[i].keylen == key.length() + 2 &&
	   !memcmp(key.c_str(), values[i].key, key.length())) {
      value_bl.append(buffer::claim_malloc(values[i].datalen, values[i].data));
      dtrace << "key=" << decode_key(string(values[i].key, values[i].keylen))
	     << dendl;
      free(values[i].key);
      i++;
    }

    count = 0;
    while (i < n_out) values[count++] = values[i++];
  } else
    finish();

  return (status == ZS_SUCCESS || status == ZS_QUERY_DONE) ? 0 : -1;
}

string ZSStore::ZSWholeSpaceIteratorImpl::key()
{
  string out_key;
  assert(cur_key.length());
  split_key(cur_key.c_str(), cur_key.length(), 0, &out_key);

  dtrace << decode_key(out_key) << dendl;

  return out_key;
}

pair<string, string> ZSStore::ZSWholeSpaceIteratorImpl::raw_key()
{
  string prefix, key;
  assert(cur_key.length());
  split_key(cur_key.c_str(), cur_key.length(), &prefix, &key);
  return make_pair(prefix, key);
}

bool ZSStore::ZSWholeSpaceIteratorImpl::raw_key_is_prefixed(
    const string &prefix)
{
  assert(cur_key.length());

  if ((cur_key.length() > prefix.length()) &&
      (cur_key[prefix.length()] == '\0'))
    return !memcmp(cur_key.c_str(), prefix.c_str(), prefix.length());

  return false;
}

/* Invalidates values.data so can be called only once */
bufferlist ZSStore::ZSWholeSpaceIteratorImpl::value()
{
  dtrace << value_bl << dendl;

  return value_bl;
}

ZSStore::ZSWholeSpaceIteratorImpl::~ZSWholeSpaceIteratorImpl()
{
  finish();

  assert(!cursor);

  if (snap_seqno) {
    ZS_status_t status;

    status = ZSDeleteContainerSnapshot(_thd_state(), cguid, snap_seqno);
    if (status != ZS_SUCCESS)
      derr << "ZSDeleteContainerSnapshot failed: " << ZSStrError(status)
	   << dendl;

    status = ZSDeleteContainerSnapshot(_thd_state(), cguid_lc, snap_seqno);
    if (status != ZS_SUCCESS)
      derr << "ZSDeleteContainerSnapshot failed: " << ZSStrError(status)
	   << dendl;
  }
}

KeyValueDB::WholeSpaceIterator ZSStore::_get_snapshot_iterator()
{
  uint64_t snap_seqno;

  ZS_status_t status;
  status = ZSCreateContainerSnapshot(_thd_state(), cguid, &snap_seqno);
  if (status != ZS_SUCCESS) {
    derr << "ZSCreateContainerSnapshot failed: " << ZSStrError(status) << dendl;
    derr << "Fallback to regular iterator instead of snapshot iterator"
	 << dendl;
    snap_seqno = 0;
  }

  status = ZSCreateContainerSnapshot(_thd_state(), cguid_lc, &snap_seqno);
  if (status != ZS_SUCCESS) {
    ZSDeleteContainerSnapshot(_thd_state(), cguid, snap_seqno);
    derr << "ZSCreateContainerSnapshot failed: " << ZSStrError(status) << dendl;
    derr << "Fallback to regular iterator instead of snapshot iterator"
	 << dendl;
    snap_seqno = 0;
  }

  dtrace << " seqno=" << snap_seqno << dendl;

  return std::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
      new ZSWholeSpaceIteratorImpl(this, cguid, cguid_lc, snap_seqno));
}

int ZSStore::ZSWholeSpaceIteratorImpl::upper_bound(const std::string &prefix,
						   const std::string &after)
{
  string k = combine_strings(prefix, after, false);
  dtrace << "upper_bound " << decode_key(k) << dendl;
  return seek(k.c_str(), k.length(), true, false);
}

int ZSStore::ZSWholeSpaceIteratorImpl::lower_bound(const std::string &prefix,
						   const std::string &to)
{
  string k = combine_strings(prefix, to, false);
  dtrace << "lower_bound " << decode_key(k) << dendl;
  return seek(k.c_str(), k.length(), true, true);
}
