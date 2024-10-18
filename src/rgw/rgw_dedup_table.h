#pragma once
#include <mutex>
#include <cstdint>
#include "common/dout.h"
//#include <cstring>
//#include <iostream>
//#include "common/dout.h"
//#include "rgw_common.h"
#include "rgw_dedup_store.h"
namespace rgw::dedup {

  // 22 Bytes key
  struct key_t {
    key_t() { ;}
    key_t(uint64_t _md5_high,
	  uint64_t _md5_low,
	  uint32_t _size_4k_units,
	  uint16_t _num_parts) {
      md5_high      = _md5_high;
      md5_low       = _md5_low;
      size_4k_units = _size_4k_units;
      num_parts     = _num_parts;
    }
    bool operator==(const struct key_t& other) const {
      return (this->md5_high       == other.md5_high &&
	      this->md5_low        == other.md5_low  &&
	      this->size_4k_units  == other.size_4k_units &&
	      this->num_parts      == other.num_parts);
    }

    bool operator!=(const struct key_t& other) const {
      return !operator==(other);
    }

    uint64_t hash() const {
      // The MD5 is already a hashing function so no need for another hash
      return this->md5_low;
    }

    uint64_t md5_high;      // High Bytes of the Object Data MD5
    uint64_t md5_low;       // Low  Bytes of the Object Data MD5
    uint32_t size_4k_units; // Object size in 4KB units max out at 16TB (AWS MAX-SIZE is 5TB)
    uint16_t num_parts;     // How many parts were used in multipart upload (AWS MAX-PART is 10,000)
  } __attribute__((__packed__));

  class dedup_table_t {
  public:
    // 6 Bytes Value
    struct value_t {
      value_t() {
	this->block_idx = 0xFFFFFFFF;
	this->count  = 0;
	this->pad_16 = 0;
	this->rec_id = 0xFF;
	this->flags.clear();

      }

      value_t(disk_block_id_t block_id, record_id_t rec_id, bool shared_manifest, bool valid_sha256) {
	this->block_idx = block_id;
	this->count  = 1;
	this->pad_16 = 0;
	this->rec_id = rec_id;
	this->flags.clear();
	this->flags.set_singleton_occupied();
	if (shared_manifest) {
	  flags.set_shared_manifest();
	}
	if (valid_sha256) {
	  flags.set_valid_sha256();
	}
      }

      inline void clear_flags() { flags.clear(); }
      inline bool has_valid_sha256() const { return flags.has_valid_sha256(); }
      inline void set_valid_sha256() { this->flags.set_valid_sha256(); }
      inline bool has_shared_manifest() const {return flags.has_shared_manifest(); }
      inline void set_shared_manifest() { this->flags.set_shared_manifest(); }
      inline bool is_singleton() const { return flags.is_singleton(); }
      inline void clear_singleton() { this->flags.clear_singleton(); }
      inline bool is_occupied() const { return flags.is_occupied(); }
      inline void set_occupied() { this->flags.set_occupied();  }
      inline void clear_occupied() { this->flags.clear_occupied(); }
      inline void set_singleton_occupied() { this->flags.set_singleton_occupied(); }

      disk_block_id_t block_idx;
      uint16_t        count;
      uint16_t        pad_16;
      record_id_t     rec_id;	// TBD use 16bits rec_id
      dedup_flags_t   flags;
    } __attribute__((__packed__));

    dedup_table_t(uint32_t entries_count, const DoutPrefixProvider* _dpp);
    ~dedup_table_t();
    void reset();
    uint32_t find_entry(const key_t *p_key);
    int add_entry(key_t *p_key, disk_block_id_t block_id, record_id_t rec_id,
		  bool shared_manifest, bool has_sha256);
    int  get_val(const key_t *p_key, struct value_t *p_val /*OUT*/);

    int set_shared_manifest_mode(const key_t *p_key,
				 disk_block_id_t block_id,
				 record_id_t rec_id);
    void count_duplicates(uint64_t *p_singleton_count,
			  uint64_t *p_unique_count,
			  uint64_t *p_duplicate_count);
    void remove_singletons_and_redistribute_keys();
    void print_redistribute_stats();
    void stat_counters_reset();

  private:
    // 28 Bytes unified entries
    struct table_entry_t {
      key_t key;
      value_t val;
    } __attribute__((__packed__));

    uint32_t       entries_count = 0;
    uint32_t       occupied_count = 0;
    table_entry_t *hash_tab = nullptr;
    std::mutex  table_mtx;

    // stat counters
    uint64_t redistributed_count = 0;
    uint64_t redistributed_search_total = 0;
    uint64_t redistributed_search_max = 0;
    uint64_t redistributed_loopback = 0;
    uint64_t redistributed_perfect = 0;
    uint64_t redistributed_clear = 0;
    uint64_t redistributed_not_needed = 0;
    const DoutPrefixProvider* dpp;
  };

} //namespace rgw::dedup
