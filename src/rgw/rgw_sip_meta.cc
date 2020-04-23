
#include "common/debug.h"

#include "rgw_sip_meta.h"
#include "rgw_metadata.h"
#include "rgw_mdlog.h"
#include "rgw_b64.h"

#include "services/svc_mdlog.h"

#define dout_subsys ceph_subsys_rgw


int SIProvider_MetaFull::init()
{
  int r = get_all_sections();
  if (r < 0) {
    lderr(cct) << __func__ << "(): get_all_sections() returned r=" << r << dendl;
    return r;
  }

  rearrange_sections();

  std::string prev;

  for (auto& s : sections) {
    next_section_map[prev] = s;
    prev = s;
  }

  return 0;
}

void SIProvider_MetaFull::append_section_from_set(set<string>& all_sections, const string& name) {
  set<string>::iterator iter = all_sections.find(name);
  if (iter != all_sections.end()) {
    sections.emplace_back(std::move(*iter));
    all_sections.erase(iter);
  }
}

/*
 * meta sync should go in the following order: user, bucket.instance, bucket
 * then whatever other sections exist (if any)
 */
void SIProvider_MetaFull::rearrange_sections() {
  set<string> all_sections;
  std::move(sections.begin(), sections.end(),
            std::inserter(all_sections, all_sections.end()));
  sections.clear();

  append_section_from_set(all_sections, "user");
  append_section_from_set(all_sections, "bucket.instance");
  append_section_from_set(all_sections, "bucket");

  std::move(all_sections.begin(), all_sections.end(),
            std::back_inserter(sections));
}

int SIProvider_MetaFull::get_all_sections() {
  void *handle;

  int ret = meta.mgr->list_keys_init(string(), string(), &handle); /* iterate top handler */
  if (ret < 0) {
    lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
    return ret;
  }

  std::list<string> result;
  bool truncated;
  int max = 32;

  do {
    ret = meta.mgr->list_keys_next(handle, max, result,
                                   &truncated);
    if (ret < 0) {
      lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
      return ret;
    }
    std::move(result.begin(), result.end(),
              std::inserter(sections, sections.end()));
    result.clear();
  } while (truncated);

  return 0;
}

int SIProvider_MetaFull::next_section(const std::string& section, string *next)
{
  auto iter = next_section_map.find(section);
  if (iter == next_section_map.end()) {
    if (section.empty()) {
      ldout(cct, 5) << "ERROR: " << __func__ << "(): next_section_map() is not initialized" << dendl;
      return -EINVAL;
    }
    return -ENOENT;
  }
  *next = iter->second;
  return 0;
}

std::string SIProvider_MetaFull::to_marker(const std::string& section, const std::string& k) const
{
  return section + "/" + k;
}

int SIProvider_MetaFull::fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  if (shard_id > 0) {
    return -ERANGE;
  }

  string section;
  string m;

  if (!marker.empty()) {
    auto pos = marker.find("/");
    if (pos == string::npos) {
      return -EINVAL;
    }
    section = marker.substr(0, pos);
    m = marker.substr(pos + 1);
    if (!m.empty()) {
      m = rgw::from_base64(m);
    }
  } else {
    int r = next_section(section, &section);
    if (r < 0) {
      return r;
    }
  }

  void *handle;

  result->done = false;
  result->more = true;

  do {
    int ret = meta.mgr->list_keys_init(section, m, &handle);
    if (ret < 0) {
      lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
      return ret;
    }

    std::list<RGWMetadataHandler::KeyInfo> entries;
    bool truncated;
    int max = 32;

    ret = meta.mgr->list_keys_next(handle, max, entries,
                                   &truncated);
    if (ret < 0) {
      lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
      return ret;
    }

    if (!entries.empty()) {
      max -= entries.size();

      m = entries.back().marker;

      for (auto& k : entries) {
        auto e = create_entry(section, k.marker);
        result->entries.push_back(e);
      }
    }

    if (!truncated) {
      ret = next_section(section, &section);
      if (ret == -ENOENT) {
        result->done = true;
        result->more = false;
        break;
      }
      m.clear();
      new_section = true;
    }

    if (max == 0) {
      break;
    }

  } while (true);


  return 0;
}

int SIProvider_MetaInc::init()
{
  meta_log = mdlog->get_log(period_id);
  num_shards = cct->_conf->rgw_md_log_max_shards;

  return 0;
}

SIProvider::Info SIProvider_MetaInc::get_info() const
{
  return { SIProvider::Type::INC, num_shards };
}

int SIProvider_MetaInc::fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  if (shard_id >= stage_info.num_shards) {
    return -ERANGE;
  }

  utime_t start_time;
  utime_t end_time;

  void *handle;

  meta_log->init_list_entries(shard_id, start_time.to_real_time(), end_time.to_real_time(), marker, &handle);
  bool truncated;
  do {
    list<cls_log_entry> entries;
    int ret = meta_log->list_entries(handle, max, entries, NULL, &truncated);
    if (ret < 0) {
      lderr(cct) << "ERROR: meta_log->list_entries() failed: ret=" << ret << dendl;
      return -ret;
    }

    max -= entries.size();

    for (auto& entry : entries) {
      siprovider_meta_info meta_info = { entry.section, entry.name };

      SIProvider::Entry e;
      e.key = entry.id;
      meta_info.encode(e.data);
      result->entries.push_back(e);
    }
  } while (truncated && max > 0);

  result->done = false; /* FIXME */
  result->more = truncated;

  meta_log->complete_list_entries(handle);

  return 0;
}


int SIProvider_MetaInc::get_start_marker(int shard_id, std::string *marker) const
{
  marker->clear();
  return 0;
}

int SIProvider_MetaInc::get_cur_state(int shard_id, std::string *marker) const
{
#warning FIXME
  return 0;
}
