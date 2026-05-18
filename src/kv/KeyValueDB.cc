// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <filesystem>
#include <memory>
#include <sstream>

#include "KeyValueDB.h"
#include "RocksDBStore.h"

using std::map;
using std::string;

KeyValueDB *KeyValueDB::create(CephContext *cct, const string& type,
			       const string& dir,
			       map<string,string> options,
			       void *p)
{
  if (type == "rocksdb") {
    return new RocksDBStore(cct, dir, options, p);
  }
  return NULL;
}

bool KeyValueDB::restore_backup(CephContext *cct,
                                const std::string &type,
                                const std::string &path,
                                const std::string &backup_location,
                                const std::optional<uint32_t> &version)
{
  if (std::filesystem::exists(path) &&
      !std::filesystem::is_empty(path)) {
    std::unique_ptr<KeyValueDB> probe(KeyValueDB::create(cct, type, path));
    if (!probe) {
      lderr(cct) << __func__ << " unsupported kv backend: " << type << dendl;
      return false;
    }
    std::ostringstream err;
    if (probe->open(err) < 0) {
      // Heuristic: rocksdb's PosixEnv lock path surfaces "lock" in the
      // error text. Any other open failure (corruption, I/O) is precisely
      // why a restore is being run -- warn and proceed.
      const std::string msg = err.str();
      if (msg.find("lock") != std::string::npos) {
        lderr(cct) << __func__ << " another monitor is using this data dir: "
                   << path << ": " << msg << dendl;
        return false;
      }
      lderr(cct) << __func__ << " existing store at " << path
                 << " is unreadable, proceeding with restore: " << msg << dendl;
    } else {
      probe->close();
    }
  }
  if (type == "rocksdb") {
    return RocksDBStore::restore_backup(cct, path, backup_location, version);
  }
  return false;
}

std::optional<std::vector<KeyValueDB::BackupStats>> KeyValueDB::list_backups(
  CephContext *cct, const std::string &type, const std::string &backup_location)
{
  if (type == "rocksdb") {
    return RocksDBStore::list_backups(cct, backup_location);
  }
  lderr(cct) << __func__ << " unsupported kv backend: " << type << dendl;
  return std::nullopt;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
  if (type == "rocksdb") {
    return RocksDBStore::_test_init(dir);
  }
  return -EINVAL;
}
