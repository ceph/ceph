#include "rgw_dedup_table.h"
#include "include/ceph_assert.h"
#include <cstring>
#include <iostream>

static uint64_t max   = 0;
static uint64_t total = 0;

namespace rgw::dedup {

  //---------------------------------------------------------------------------
  dedup_table_t::dedup_table_t(uint32_t _entries_count, const DoutPrefixProvider* _dpp)
  {
    dpp = _dpp;
    entries_count = _entries_count;
    hash_tab = new table_entry_t[entries_count];
    ceph_assert(hash_tab);
    reset();
  }

  //---------------------------------------------------------------------------
  dedup_table_t::~dedup_table_t()
  {
    if (hash_tab) {
      delete [] hash_tab;
    }
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::reset()
  {
    char *p = (char*)hash_tab;
    memset(p, 0, sizeof(table_entry_t)*entries_count);
    occupied_count = 0;
    stat_counters_reset();
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::remove_singletons_and_redistribute_keys()
  {
    for (uint32_t tab_idx = 0; tab_idx < entries_count; tab_idx++) {
      if (!hash_tab[tab_idx].val.is_occupied()) {
	continue;
      }

      if (hash_tab[tab_idx].val.is_singleton()) {
	hash_tab[tab_idx].val.clear_flags();
	redistributed_clear++;
	continue;
      }

      const key_t &key = hash_tab[tab_idx].key;
      uint32_t key_idx = key.hash() % entries_count;
      if (key_idx != tab_idx) {
	uint64_t count = 1;
	redistributed_count++;
	uint32_t idx = key_idx;
	while (hash_tab[idx].val.is_occupied()   &&
	       !hash_tab[idx].val.is_singleton() &&
	       (hash_tab[idx].key != key)) {
	  count++;
	  idx = (idx + 1) % entries_count;
	}

	if (idx != tab_idx) {
	  if (hash_tab[idx].val.is_occupied() && hash_tab[idx].val.is_singleton() ) {
	    redistributed_clear++;
	  }
	  if (idx == key_idx) {
	    redistributed_perfect++;
	  }
	  hash_tab[idx] = hash_tab[tab_idx];
	  hash_tab[tab_idx].val.clear_flags();
	}
	else {
	  redistributed_loopback++;
	}

	redistributed_search_max = std::max(redistributed_search_max, count);
	redistributed_search_total += count;
      }
      else {
	redistributed_not_needed++;
      }
    }
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::stat_counters_reset()
  {
    redistributed_count = 0;
    redistributed_search_total = 0;
    redistributed_search_max = 0;
    redistributed_loopback = 0;
    redistributed_perfect = 0;
    redistributed_clear = 0;
    redistributed_not_needed = 0;
  }

  //---------------------------------------------------------------------------
  uint32_t dedup_table_t::find_entry(const key_t *p_key)
  {
    uint64_t count = 1;
    //const std::lock_guard<std::mutex> lock(table_mtx);
    uint32_t idx = p_key->hash() % entries_count;

    // search until we either find the key, or find an empty slot.
    while (hash_tab[idx].val.is_occupied() && (hash_tab[idx].key != *p_key)) {
      count++;
      idx = (idx + 1) % entries_count;
    }
    max = std::max(max, count);
    total += count;
    return idx;
  }

  //---------------------------------------------------------------------------
  int dedup_table_t::add_entry(key_t *p_key, disk_block_id_t block_id, record_id_t rec_id,
			       bool shared_manifest, bool valid_sha256)
  {
    if (occupied_count < entries_count) {
      occupied_count++;
    }
    else {
      // overflow!
      return -1;
    }
    value_t val(block_id, rec_id, shared_manifest, valid_sha256);
    const std::lock_guard<std::mutex> lock(table_mtx);
    uint32_t idx = find_entry(p_key);
    if (!hash_tab[idx].val.is_occupied()) {
      hash_tab[idx].key = *p_key;
      hash_tab[idx].val = val;
      ldpp_dout(dpp, 20) << __func__ << "::add new entry" << dendl;
      ceph_assert(hash_tab[idx].val.count == 1);
    }
    else {
      ceph_assert(hash_tab[idx].key == *p_key);
      hash_tab[idx].val.count ++;
      if (!hash_tab[idx].val.has_shared_manifest() && shared_manifest) {
	// replace value!
	ldpp_dout(dpp, 20) << __func__ << "::Replace with shared_manifest" << dendl;
	val.count = hash_tab[idx].val.count;
	val.clear_singleton();
	hash_tab[idx].val = val;
      }
      else if (hash_tab[idx].val.is_singleton()) {
	ldpp_dout(dpp, 20) << __func__ << "::clear singleton" << dendl;
	// This is the second record with the same key -> clear singleton state
	hash_tab[idx].val.clear_singleton();
      }
      ceph_assert(hash_tab[idx].val.count > 1);
    }
    ldpp_dout(dpp, 20) << __func__ << "::DUP COUNT=" << hash_tab[idx].val.count << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  int dedup_table_t::set_shared_manifest_mode(const key_t *p_key,
					      disk_block_id_t block_id,
					      record_id_t rec_id)
  {
    const std::lock_guard<std::mutex> lock(table_mtx);
    uint32_t idx = find_entry(p_key);
    value_t &val = hash_tab[idx].val;
    if (val.is_occupied()) {
      if (val.block_idx == block_id && val.rec_id == rec_id) {
	val.set_shared_manifest();
	return 0;
      }
    }

    return -1;
  }

  //---------------------------------------------------------------------------
  int dedup_table_t::get_val(const key_t *p_key, struct value_t *p_val /*OUT*/)
  {
    const std::lock_guard<std::mutex> lock(table_mtx);
    uint32_t idx = find_entry(p_key);
    const value_t &val = hash_tab[idx].val;
    if (!val.is_occupied()) {
      return -1;
    }

    *p_val = val;
    return 0;
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::count_duplicates(uint64_t *p_singleton_count,
				       uint64_t *p_unique_count,
				       uint64_t *p_duplicate_count)
  {
    for (uint32_t tab_idx = 0; tab_idx < entries_count; tab_idx++) {
      if (!hash_tab[tab_idx].val.is_occupied()) {
	continue;
      }

      if (hash_tab[tab_idx].val.is_singleton()) {
	(*p_singleton_count)++;
      }
      else {
	(*p_duplicate_count) += (hash_tab[tab_idx].val.count -1);
	(*p_unique_count) ++;
      }
    }
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::print_redistribute_stats()
  {
    std::cout << "sizeof(entry)=" << sizeof(table_entry_t) << std::endl;
    double avg = (double)redistributed_search_total / entries_count;
    std::cout << "redistribute::num entries=" << entries_count << ", total=" << redistributed_search_total
	      << ", avg=" << avg << ", max=" << redistributed_search_max << std::endl;
    std::cout << "Entries redistributed=" << redistributed_count
	      << ", redistributed_clear=" << redistributed_clear
	      << ", loopback=" << redistributed_loopback
	      << ", not needed=" << redistributed_not_needed
	      << ", perfect placment=" << redistributed_perfect << std::endl;
  }
} // namespace rgw::dedup

#if 0
#include <climits>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <iomanip>
#include <random>

//---------------------------------------------------------------------------
int main()
{
  static constexpr unsigned MAX_ENTRIES = 1024;
  rgw::dedup::key_t *key_tab = new rgw::dedup::key_t[MAX_ENTRIES];
  if (!key_tab) {
    std::cerr << "faild alloc!" << std::endl;
    return 1;
  }
  rgw::dedup::key_t *p_key = key_tab;
  //rgw::dedup::dedup_table_t tab(MAX_ENTRIES + MAX_ENTRIES/5);
  rgw::dedup::dedup_table_t tab(MAX_ENTRIES);

  std::cout << "sizeof(key)=" << sizeof(rgw::dedup::key_t) << std::endl;
  // Seed with a real random value, if available
  std::random_device r;
  // Choose a random mean between 1 ULLONG_MAX
  std::default_random_engine e1(r());
  std::uniform_int_distribution<uint64_t> uniform_dist(1, std::numeric_limits<uint64_t>::max());

  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    uint64_t md5_high  = uniform_dist(e1);
    uint64_t md5_low   = uniform_dist(e1);
    uint32_t size_4k_units  = std::rand();
    uint16_t num_parts = std::rand();
    //std::cout << std::hex << md5_high << "::" << md5_low << "::" << block_id << std::endl;
    rgw::dedup::key_t key(md5_high, md5_low, size_4k_units, num_parts);
    *p_key = key;
    p_key++;
  }
  work_shard_t work_shard = 3;
  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    disk_block_id_t block_id(worker_id, std::rand());
    tab.add_entry(key_tab+i, block_id, 0, false, false);
  }
  double avg = (double)total / MAX_ENTRIES;
  std::cout << "Insert::num entries=" << MAX_ENTRIES << ", total=" << total
	    << ", avg=" << avg << ", max=" << max << std::endl;
  std::cout << "==========================================\n";

  total = 0;
  max = 0;
  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    tab.find_entry(key_tab+i);
  }
  avg = (double)total / MAX_ENTRIES;
  std::cout << "Find::num entries=" << MAX_ENTRIES << ", total=" << total
	    << ", avg=" << avg << ", max=" << max << std::endl;
  std::cout << "==========================================\n";
  tab.remove_singletons_and_redistribute_keys();
  tab.print_redistribute_stats();
  tab.stat_counters_reset();
  std::cout << "==========================================\n";
  total = 0;
  max = 0;
  uint32_t cnt = 0;
  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    rgw::dedup::key_t *p_key = key_tab+i;
    tab.find_entry(p_key);
    cnt++;
#if 0
    if (p_key->md5_high % 5 == 0) {
      tab.find_entry(p_key);
      cnt++;
    }
#endif
  }
  avg = (double)total / cnt;
  std::cout << "num entries=" << cnt << ", total=" << total
	    << ", avg=" << avg << ", max=" << max << std::endl;
}
#endif
