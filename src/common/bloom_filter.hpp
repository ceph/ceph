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

#include <cstddef>
#include <algorithm>
#include <cmath>
#include <limits>
#include <list>
#include <string>
#include <vector>

#include "include/encoding.h"
#include "common/Formatter.h"

static const std::size_t bits_per_char = 0x08;    // 8 bits in 1 char(unsigned)
static const unsigned char bit_mask[bits_per_char] = {
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

  typedef unsigned int bloom_type;
  typedef unsigned char cell_type;

  unsigned char*       bit_table_;   ///< pointer to bit map
  std::vector<bloom_type> salt_;     ///< vector of salts
  std::size_t         salt_count_;   ///< number of salts
  std::size_t         table_size_;   ///< bit table size in bytes
  std::size_t         insert_count_;  ///< insertion count
  std::size_t         target_element_count_;  ///< target number of unique insertions
  std::size_t         random_seed_;  ///< random seed

public:

  bloom_filter()
    : bit_table_(0),
      salt_count_(0),
      table_size_(0),
      insert_count_(0),
      target_element_count_(0),
      random_seed_(0)
  {}

  bloom_filter(const std::size_t& predicted_inserted_element_count,
	       const double& false_positive_probability,
	       const std::size_t& random_seed)
    : bit_table_(0),
      insert_count_(0),
      target_element_count_(predicted_inserted_element_count),
      random_seed_((random_seed) ? random_seed : 0xA5A5A5A5)
  {
    assert(false_positive_probability > 0.0);
    find_optimal_parameters(predicted_inserted_element_count, false_positive_probability,
			    &salt_count_, &table_size_);
    init();
  }

  bloom_filter(const std::size_t& salt_count,
	       std::size_t table_size,
	       const std::size_t& random_seed,
	       std::size_t target_element_count)
    : bit_table_(0),
      salt_count_(salt_count),
      table_size_(table_size),
      insert_count_(0),
      target_element_count_(target_element_count),
      random_seed_((random_seed) ? random_seed : 0xA5A5A5A5)
  {
    init();
  }

  void init() {
    generate_unique_salt();
    if (table_size_) {
      bit_table_ = new cell_type[table_size_];
      std::fill_n(bit_table_, table_size_, 0x00);
    } else {
      bit_table_ = NULL;
    }
  }

  bloom_filter(const bloom_filter& filter)
    : bit_table_(0)
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
      delete[] bit_table_;
      bit_table_ = new cell_type[table_size_];
      std::copy(filter.bit_table_, filter.bit_table_ + table_size_, bit_table_);
      salt_ = filter.salt_;
    }
    return *this;
  }

  virtual ~bloom_filter()
  {
    delete[] bit_table_;
  }

  inline bool operator!() const
  {
    return (0 == table_size_);
  }

  inline void clear()
  {
    if (bit_table_)
      std::fill_n(bit_table_, table_size_, 0x00);
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
    assert(bit_table_);
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(val,salt_[i]),bit_index,bit);
      bit_table_[bit_index >> 3] |= bit_mask[bit];
    }
    ++insert_count_;
  }

  inline void insert(const unsigned char* key_begin, const std::size_t& length)
  {
    assert(bit_table_);
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(key_begin,length,salt_[i]),bit_index,bit);
      bit_table_[bit_index >> 3] |= bit_mask[bit];
    }
    ++insert_count_;
  }

  template<typename T>
  inline void insert(const T& t)
  {
    // Note: T must be a C++ POD type.
    insert(reinterpret_cast<const unsigned char*>(&t),sizeof(T));
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
    if (!bit_table_)
      return false;
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(val,salt_[i]),bit_index,bit);
      if ((bit_table_[bit_index >> 3] & bit_mask[bit]) != bit_mask[bit])
      {
        return false;
      }
    }
    return true;
  }

  inline virtual bool contains(const unsigned char* key_begin, const std::size_t length) const
  {
    if (!bit_table_)
      return false;
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(key_begin,length,salt_[i]),bit_index,bit);
      if ((bit_table_[bit_index >> 3] & bit_mask[bit]) != bit_mask[bit])
      {
        return false;
      }
    }
    return true;
  }

  template<typename T>
  inline bool contains(const T& t) const
  {
    return contains(reinterpret_cast<const unsigned char*>(&t),static_cast<std::size_t>(sizeof(T)));
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
    return table_size_ * bits_per_char;
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
  inline double density() const
  {
    if (!bit_table_)
      return 0.0;
    size_t set = 0;
    uint8_t *p = bit_table_;
    size_t left = table_size_;
    while (left-- > 0) {
      uint8_t c = *p;
      for (; c; ++set)
	c &= c - 1;
      ++p;
    }
    return (double)set / (double)(table_size_ << 3);
  }

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
    return bit_table_;
  }

protected:

  inline virtual void compute_indices(const bloom_type& hash, std::size_t& bit_index, std::size_t& bit) const
  {
    bit_index = hash % (table_size_ << 3);
    bit = bit_index & 7;
  }

  void generate_unique_salt()
  {
    /*
      Note:
      A distinct hash function need not be implementation-wise
      distinct. In the current implementation "seeding" a common
      hash function with different values seems to be adequate.
    */
    const unsigned int predef_salt_count = 128;
    static const bloom_type predef_salt[predef_salt_count] = {
      0xAAAAAAAA, 0x55555555, 0x33333333, 0xCCCCCCCC,
      0x66666666, 0x99999999, 0xB5B5B5B5, 0x4B4B4B4B,
      0xAA55AA55, 0x55335533, 0x33CC33CC, 0xCC66CC66,
      0x66996699, 0x99B599B5, 0xB54BB54B, 0x4BAA4BAA,
      0xAA33AA33, 0x55CC55CC, 0x33663366, 0xCC99CC99,
      0x66B566B5, 0x994B994B, 0xB5AAB5AA, 0xAAAAAA33,
      0x555555CC, 0x33333366, 0xCCCCCC99, 0x666666B5,
      0x9999994B, 0xB5B5B5AA, 0xFFFFFFFF, 0xFFFF0000,
      0xB823D5EB, 0xC1191CDF, 0xF623AEB3, 0xDB58499F,
      0xC8D42E70, 0xB173F616, 0xA91A5967, 0xDA427D63,
      0xB1E8A2EA, 0xF6C0D155, 0x4909FEA3, 0xA68CC6A7,
      0xC395E782, 0xA26057EB, 0x0CD5DA28, 0x467C5492,
      0xF15E6982, 0x61C6FAD3, 0x9615E352, 0x6E9E355A,
      0x689B563E, 0x0C9831A8, 0x6753C18B, 0xA622689B,
      0x8CA63C47, 0x42CC2884, 0x8E89919B, 0x6EDBD7D3,
      0x15B6796C, 0x1D6FDFE4, 0x63FF9092, 0xE7401432,
      0xEFFE9412, 0xAEAEDF79, 0x9F245A31, 0x83C136FC,
      0xC3DA4A8C, 0xA5112C8C, 0x5271F491, 0x9A948DAB,
      0xCEE59A8D, 0xB5F525AB, 0x59D13217, 0x24E7C331,
      0x697C2103, 0x84B0A460, 0x86156DA9, 0xAEF2AC68,
      0x23243DA5, 0x3F649643, 0x5FA495A8, 0x67710DF8,
      0x9A6C499E, 0xDCFB0227, 0x46A43433, 0x1832B07A,
      0xC46AFF3C, 0xB9C8FFF0, 0xC9500467, 0x34431BDF,
      0xB652432B, 0xE367F12B, 0x427F4C1B, 0x224C006E,
      0x2E7E5A89, 0x96F99AA5, 0x0BEB452A, 0x2FD87C39,
      0x74B2E1FB, 0x222EFD24, 0xF357F60C, 0x440FCB1E,
      0x8BBE030F, 0x6704DC29, 0x1144D12F, 0x948B1355,
      0x6D8FD7E9, 0x1C11A014, 0xADD1592F, 0xFB3C712E,
      0xFC77642F, 0xF9C4CE8C, 0x31312FB9, 0x08B0DD79,
      0x318FA6E7, 0xC040D23D, 0xC0589AA7, 0x0CA5C075,
      0xF874B172, 0x0CF914D5, 0x784D3280, 0x4E8CFEBC,
      0xC569F575, 0xCDB2A091, 0x2CC016B4, 0x5C5F4421
    };

    if (salt_count_ <= predef_salt_count)
    {
      std::copy(predef_salt,
		predef_salt + salt_count_,
		std::back_inserter(salt_));
       for (unsigned int i = 0; i < salt_.size(); ++i)
       {
        /*
          Note:
          This is done to integrate the user defined random seed,
          so as to allow for the generation of unique bloom filter
          instances.
        */
        salt_[i] = salt_[i] * salt_[(i + 3) % salt_.size()] + random_seed_;
       }
    }
    else
    {
      std::copy(predef_salt,predef_salt + predef_salt_count,
		std::back_inserter(salt_));
      srand(static_cast<unsigned int>(random_seed_));
      while (salt_.size() < salt_count_)
      {
        bloom_type current_salt = static_cast<bloom_type>(rand()) * static_cast<bloom_type>(rand());
        if (0 == current_salt)
	  continue;
        if (salt_.end() == std::find(salt_.begin(), salt_.end(), current_salt))
        {
          salt_.push_back(current_salt);
        }
      }
    }
  }

  static void find_optimal_parameters(std::size_t target_insert_count,
				      double target_fpp,
				      std::size_t *salt_count,
				      std::size_t *table_size)
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

    *salt_count = static_cast<std::size_t>(min_k);
    size_t t = static_cast<std::size_t>(min_m);
    t += (((t & 7) != 0) ? (bits_per_char - (t & 7)) : 0);
    *table_size = t >> 3;
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
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<bloom_filter*>& ls);
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

  inline virtual std::size_t size() const
  {
    return size_list.back() * bits_per_char;
  }

  inline bool compress(const double& target_ratio)
  {
    if (!bit_table_)
      return false;

    if ((0.0 >= target_ratio) || (target_ratio >= 1.0))
    {
      return false;
    }

    std::size_t original_table_size = size_list.back();
    std::size_t new_table_size = static_cast<std::size_t>(size_list.back() * target_ratio);

    if ((!new_table_size) || (new_table_size >= original_table_size))
    {
      return false;
    }

    cell_type* tmp = new cell_type[new_table_size];
    std::copy(bit_table_, bit_table_ + (new_table_size), tmp);
    cell_type* itr = bit_table_ + (new_table_size);
    cell_type* end = bit_table_ + (original_table_size);
    cell_type* itr_tmp = tmp;
    cell_type* itr_end = tmp + (new_table_size);
    while (end != itr)
    {
      *(itr_tmp++) |= (*itr++);
      if (itr_tmp == itr_end)
	itr_tmp = tmp;
    }

    delete[] bit_table_;
    bit_table_ = tmp;
    size_list.push_back(new_table_size);
    table_size_ = new_table_size;

    return true;
  }

  virtual inline double approx_unique_element_count() const {
    // this is not a very good estimate; a better solution should have
    // some asymptotic behavior as density() approaches 1.0.
    //
    // the compress() correction is also bad; it tends to under-estimate.
    return (double)target_element_count_ * 2.0 * density() * (double)size_list.back() / (double)size_list.front();
  }

private:

  inline virtual void compute_indices(const bloom_type& hash, std::size_t& bit_index, std::size_t& bit) const
  {
    bit_index = hash;
    for (std::size_t i = 0; i < size_list.size(); ++i)
    {
      bit_index %= size_list[i] << 3;
    }
    bit = bit_index & 7;
  }

  std::vector<std::size_t> size_list;
public:
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<compressible_bloom_filter*>& ls);
};
WRITE_CLASS_ENCODER(compressible_bloom_filter)

#endif


/*
  Note 1:
  If it can be guaranteed that bits_per_char will be of the form 2^n then
  the following optimization can be used:

  hash_table[bit_index >> n] |= bit_mask[bit_index & (bits_per_char - 1)];

  Note 2:
  For performance reasons where possible when allocating memory it should
  be aligned (aligned_alloc) according to the architecture being used.
*/
