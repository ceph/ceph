// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 *******************************************************************
 *                                                                 *
 *                        Open Bloom Filter                        *
 *                                                                 *
 * Author: Arash Partow - 2000                                     *
 * URL: http://www.partow.net/programming/hashfunctions/index.html *
 *                                                                 *
 * Copyright notice:                                               *
 * Free use of the Open Bloom Filter Library is permitted under    *
 * the guidelines and in accordance with the most current version  *
 * of the Boost Software License, Version 1.0                      *
 * http://www.opensource.org/licenses/bsl1.0.html                  *
 *                                                                 *
 *******************************************************************
*/


#ifndef COMMON_BLOOM_FILTER_HPP
#define COMMON_BLOOM_FILTER_HPP

#include <cmath>

#include "include/encoding.h"
#include "include/mempool.h"

static const unsigned char bit_mask[CHAR_BIT] = {
  0x01,  //00000001
  0x02,  //00000010
  0x04,  //00000100
  0x08,  //00001000
  0x10,  //00010000
  0x20,  //00100000
  0x40,  //01000000
  0x80   //10000000
};

class bloom_filter
{
protected:

  using bloom_type = unsigned int;
  using cell_type = unsigned char;
  using table_type = mempool::bloom_filter::vector<cell_type>;

  std::vector<bloom_type> salt_;     ///< vector of salts
  table_type          bit_table_;    ///< bit map
  std::size_t         salt_count_;   ///< number of salts
  std::size_t         table_size_;   ///< bit table size in bytes
  std::size_t         insert_count_;  ///< insertion count
  std::size_t         target_element_count_;  ///< target number of unique insertions
  std::size_t         random_seed_;  ///< random seed

public:

  bloom_filter()
    : salt_count_(0),
      table_size_(0),
      insert_count_(0),
      target_element_count_(0),
      random_seed_(0)
  {}

  bloom_filter(const std::size_t& predicted_inserted_element_count,
	       const double& false_positive_probability,
	       const std::size_t& random_seed)
    : insert_count_(0),
      target_element_count_(predicted_inserted_element_count),
      random_seed_((random_seed) ? random_seed : 0xA5A5A5A5)
  {
    ceph_assert(false_positive_probability > 0.0);
    std::tie(salt_count_, table_size_) =
      find_optimal_parameters(predicted_inserted_element_count,
			      false_positive_probability);
    init();
  }

  bloom_filter(const std::size_t& salt_count,
	       std::size_t table_size,
	       const std::size_t& random_seed,
	       std::size_t target_element_count)
    : salt_count_(salt_count),
      table_size_(table_size),
      insert_count_(0),
      target_element_count_(target_element_count),
      random_seed_((random_seed) ? random_seed : 0xA5A5A5A5)
  {
    init();
  }

  void init() {
    generate_unique_salt();
    bit_table_.resize(table_size_, static_cast<unsigned char>(0x00));
  }

  bloom_filter(const bloom_filter& filter)
  {
    this->operator=(filter);
  }

  bloom_filter& operator = (const bloom_filter& filter)
  {
    if (this != &filter) {
      salt_count_ = filter.salt_count_;
      table_size_ = filter.table_size_;
      insert_count_ = filter.insert_count_;
      target_element_count_ = filter.target_element_count_;
      random_seed_ = filter.random_seed_;
      bit_table_ = filter.bit_table_;
      salt_ = filter.salt_;
    }
    return *this;
  }

  virtual ~bloom_filter() = default;

  inline bool operator!() const
  {
    return (0 == table_size_);
  }

  inline void clear()
  {
    std::fill(bit_table_.begin(), bit_table_.end(),
	      static_cast<unsigned char>(0x00));
    insert_count_ = 0;
  }

  /**
   * insert a u32 into the set
   *
   * NOTE: the internal hash is weak enough that consecutive inputs do
   * not achieve the desired fpp.  Well-mixed values should be used
   * here (e.g., put rjhash(x) into the filter instead of just x).
   *
   * @param val integer value to insert
   */
  inline void insert(uint32_t val) {
    for (auto salt : salt_) {
      auto [bit_index, bit] = compute_indices(hash_ap(val, salt));
      bit_table_[bit_index >> 3] |= bit_mask[bit];
    }
    ++insert_count_;
  }

  inline void insert(const unsigned char* key_begin, const std::size_t& length)
  {
    for (auto salt : salt_) {
      auto [bit_index, bit] = compute_indices(hash_ap(key_begin, length, salt));
      bit_table_[bit_index >> 3] |= bit_mask[bit];
    }
    ++insert_count_;
  }

  inline void insert(const std::string& key)
  {
    insert(reinterpret_cast<const unsigned char*>(key.c_str()),key.size());
  }

  inline void insert(const char* data, const std::size_t& length)
  {
    insert(reinterpret_cast<const unsigned char*>(data),length);
  }

  template<typename InputIterator>
  inline void insert(const InputIterator begin, const InputIterator end)
  {
    InputIterator itr = begin;
    while (end != itr)
    {
      insert(*(itr++));
    }
  }

  /**
   * check if a u32 is contained by set
   *
   * NOTE: the internal hash is weak enough that consecutive inputs do
   * not achieve the desired fpp.  Well-mixed values should be used
   * here (e.g., put rjhash(x) into the filter instead of just x).
   *
   * @param val integer value to query
   * @returns true if value is (probably) in the set, false if it definitely is not
   */
  inline virtual bool contains(uint32_t val) const
  {
    if (table_size_ == 0) {
      return false;
    }
    for (auto salt : salt_) {
      auto [bit_index, bit] = compute_indices(hash_ap(val, salt));
      if ((bit_table_[bit_index >> 3] & bit_mask[bit]) != bit_mask[bit]) {
        return false;
      }
    }
    return true;
  }

  inline virtual bool contains(const unsigned char* key_begin, const std::size_t length) const
  {
    if (table_size_ == 0) {
      return false;
    }
    for (auto salt : salt_) {
      auto [bit_index, bit] = compute_indices(hash_ap(key_begin, length, salt));
      if ((bit_table_[bit_index >> 3] & bit_mask[bit]) != bit_mask[bit]) {
        return false;
      }
    }
    return true;
  }

  inline bool contains(const std::string& key) const
  {
    return contains(reinterpret_cast<const unsigned char*>(key.c_str()),key.size());
  }

  inline bool contains(const char* data, const std::size_t& length) const
  {
    return contains(reinterpret_cast<const unsigned char*>(data),length);
  }

  template<typename InputIterator>
  inline InputIterator contains_all(const InputIterator begin, const InputIterator end) const
  {
    InputIterator itr = begin;
    while (end != itr)
    {
      if (!contains(*itr))
      {
        return itr;
      }
      ++itr;
    }
    return end;
  }

  template<typename InputIterator>
  inline InputIterator contains_none(const InputIterator begin, const InputIterator end) const
  {
    InputIterator itr = begin;
    while (end != itr)
    {
      if (contains(*itr))
      {
        return itr;
      }
      ++itr;
    }
    return end;
  }

  inline virtual std::size_t size() const
  {
    return table_size_ * CHAR_BIT;
  }

  inline std::size_t element_count() const
  {
    return insert_count_;
  }

  inline bool is_full() const
  {
    return insert_count_ >= target_element_count_;
  }

  /*
   * density of bits set.  inconvenient units, but:
   *    .3  = ~50% target insertions
   *    .5  = 100% target insertions, "perfectly full"
   *    .75 = 200% target insertions
   *   1.0  = all bits set... infinite insertions
   */
  double density() const;

  virtual inline double approx_unique_element_count() const {
    // this is not a very good estimate; a better solution should have
    // some asymptotic behavior as density() approaches 1.0.
    return (double)target_element_count_ * 2.0 * density();
  }

  inline double effective_fpp() const
  {
    /*
      Note:
      The effective false positive probability is calculated using the
      designated table size and hash function count in conjunction with
      the current number of inserted elements - not the user defined
      predicated/expected number of inserted elements.
    */
    return std::pow(1.0 - std::exp(-1.0 * salt_.size() * insert_count_ / size()), 1.0 * salt_.size());
  }

  inline const cell_type* table() const
  {
    return bit_table_.data();
  }

protected:

  virtual std::pair<size_t /* bit_index */,
		    size_t /* bit */>
  compute_indices(const bloom_type& hash) const
  {
    size_t bit_index = hash % (table_size_ << 3);
    size_t bit = bit_index & 7;
    return {bit_index, bit};
  }

  void generate_unique_salt();

  static std::pair<std::size_t /* salt_count */,
		   std::size_t /* table_size */>
  find_optimal_parameters(std::size_t target_insert_count,
			  double target_fpp)
  {
    /*
      Note:
      The following will attempt to find the number of hash functions
      and minimum amount of storage bits required to construct a bloom
      filter consistent with the user defined false positive probability
      and estimated element insertion count.
    */

    double min_m = std::numeric_limits<double>::infinity();
    double min_k = 0.0;
    double curr_m = 0.0;
    double k = 1.0;
    while (k < 1000.0)
    {
      double numerator  = (- k * target_insert_count);
      double denominator = std::log(1.0 - std::pow(target_fpp, 1.0 / k));
      curr_m = numerator / denominator;

      if (curr_m < min_m)
      {
        min_m = curr_m;
        min_k = k;
      }
      k += 1.0;
    }

    size_t salt_count = static_cast<std::size_t>(min_k);
    size_t t = static_cast<std::size_t>(min_m);
    t += (((t & 7) != 0) ? (CHAR_BIT - (t & 7)) : 0);
    size_t table_size = t >> 3;
    return {salt_count, table_size};
  }

  inline bloom_type hash_ap(uint32_t val, bloom_type hash) const
  {
    hash ^=    (hash <<  7) ^  ((val & 0xff000000) >> 24) * (hash >> 3);
    hash ^= (~((hash << 11) + (((val & 0xff0000) >> 16) ^ (hash >> 5))));
    hash ^=    (hash <<  7) ^  ((val & 0xff00) >> 8) * (hash >> 3);
    hash ^= (~((hash << 11) + (((val & 0xff)) ^ (hash >> 5))));
    return hash;
  }

  inline bloom_type hash_ap(const unsigned char* begin, std::size_t remaining_length, bloom_type hash) const
  {
    const unsigned char* itr = begin;

    while (remaining_length >= 4)
    {
      hash ^=    (hash <<  7) ^  (*itr++) * (hash >> 3);
      hash ^= (~((hash << 11) + ((*itr++) ^ (hash >> 5))));
      hash ^=    (hash <<  7) ^  (*itr++) * (hash >> 3);
      hash ^= (~((hash << 11) + ((*itr++) ^ (hash >> 5))));
      remaining_length -= 4;
    }

    while (remaining_length >= 2)
    {
      hash ^=    (hash <<  7) ^  (*itr++) * (hash >> 3);
      hash ^= (~((hash << 11) + ((*itr++) ^ (hash >> 5))));
      remaining_length -= 2;
    }

    if (remaining_length)
    {
      hash ^= (hash <<  7) ^ (*itr) * (hash >> 3);
    }

    return hash;
  }

public:
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<bloom_filter> generate_test_instances();
};
WRITE_CLASS_ENCODER(bloom_filter)


class compressible_bloom_filter : public bloom_filter
{
public:

  compressible_bloom_filter() : bloom_filter() {}

  compressible_bloom_filter(const std::size_t& predicted_element_count,
			    const double& false_positive_probability,
			    const std::size_t& random_seed)
    : bloom_filter(predicted_element_count, false_positive_probability, random_seed)
  {
    size_list.push_back(table_size_);
  }

  compressible_bloom_filter(const std::size_t& salt_count,
			    std::size_t table_size,
			    const std::size_t& random_seed,
			    std::size_t target_count)
    : bloom_filter(salt_count, table_size, random_seed, target_count)
  {
    size_list.push_back(table_size_);
  }

  inline std::size_t size() const override
  {
    return size_list.back() * CHAR_BIT;
  }

  bool compress(const double& target_ratio);

  inline double approx_unique_element_count() const override {
    // this is not a very good estimate; a better solution should have
    // some asymptotic behavior as density() approaches 1.0.
    //
    // the compress() correction is also bad; it tends to under-estimate.
    return (double)target_element_count_ * 2.0 * density() * (double)size_list.back() / (double)size_list.front();
  }

private:

  std::pair<size_t /* bit_index */,
	    size_t /* bit */>
  compute_indices(const bloom_type& hash) const final
  {
    size_t bit_index = hash;
    for (auto size : size_list) {
      bit_index %= size << 3;
    }
    size_t bit = bit_index & 7;
    return {bit_index, bit};
  }

  std::vector<std::size_t> size_list;
public:
  void encode(ceph::bufferlist& bl) const;
  void decode(ceph::bufferlist::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<compressible_bloom_filter> generate_test_instances();
};
WRITE_CLASS_ENCODER(compressible_bloom_filter)

#endif


/*
  Note 1:
  If it can be guaranteed that CHAR_BIT will be of the form 2^n then
  the following optimization can be used:

  bit_table_[bit_index >> n] |= bit_mask[bit_index & (CHAR_BIT - 1)];

  Note 2:
  For performance reasons where possible when allocating memory it should
  be aligned (aligned_alloc) according to the architecture being used.
*/
