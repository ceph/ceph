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

public:

  bloom_filter()
    : bit_table_(0),
      salt_count_(0),
      table_size_(0),
      raw_table_size_(0),
      inserted_element_count_(0),
      random_seed_(0)
  {}

  bloom_filter(const std::size_t& predicted_inserted_element_count,
	       const double& false_positive_probability,
	       const std::size_t& random_seed)
    : bit_table_(0),
      inserted_element_count_(0),
      random_seed_((random_seed) ? random_seed : 0xA5A5A5A5)
  {
    find_optimal_parameters(predicted_inserted_element_count, false_positive_probability,
			    &salt_count_, &table_size_);
    init();
  }

  bloom_filter(const std::size_t& salt_count, std::size_t table_size,
	       const std::size_t& random_seed)
    : bit_table_(0),
      salt_count_(salt_count),
      table_size_(table_size),
      inserted_element_count_(0),
      random_seed_((random_seed) ? random_seed : 0xA5A5A5A5)
  {
    init();
  }

  void init() {
    generate_unique_salt();
    raw_table_size_ = table_size_ / bits_per_char;
    bit_table_ = new cell_type[raw_table_size_];
    std::fill_n(bit_table_,raw_table_size_,0x00);
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
      raw_table_size_ = filter.raw_table_size_;
      inserted_element_count_ = filter.inserted_element_count_;
      random_seed_ = filter.random_seed_;
      delete[] bit_table_;
      bit_table_ = new cell_type[raw_table_size_];
      std::copy(filter.bit_table_,filter.bit_table_ + raw_table_size_,bit_table_);
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
    std::fill_n(bit_table_,raw_table_size_,0x00);
    inserted_element_count_ = 0;
  }

  inline void insert(uint32_t val) {
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(val,salt_[i]),bit_index,bit);
      bit_table_[bit_index / bits_per_char] |= bit_mask[bit];
    }
    ++inserted_element_count_;
  }

  inline void insert(const unsigned char* key_begin, const std::size_t& length)
  {
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(key_begin,length,salt_[i]),bit_index,bit);
      bit_table_[bit_index / bits_per_char] |= bit_mask[bit];
    }
    ++inserted_element_count_;
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

  inline virtual bool contains(uint32_t val) const
  {
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(val,salt_[i]),bit_index,bit);
      if ((bit_table_[bit_index / bits_per_char] & bit_mask[bit]) != bit_mask[bit])
      {
        return false;
      }
    }
    return true;
  }

  inline virtual bool contains(const unsigned char* key_begin, const std::size_t length) const
  {
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for (std::size_t i = 0; i < salt_.size(); ++i)
    {
      compute_indices(hash_ap(key_begin,length,salt_[i]),bit_index,bit);
      if ((bit_table_[bit_index / bits_per_char] & bit_mask[bit]) != bit_mask[bit])
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
    return table_size_;
  }

  inline std::size_t element_count() const
  {
    return inserted_element_count_;
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
    return std::pow(1.0 - std::exp(-1.0 * salt_.size() * inserted_element_count_ / size()), 1.0 * salt_.size());
  }

  inline bloom_filter& operator &= (const bloom_filter& filter)
  {
    /* intersection */
    if (
	(salt_count_  == filter.salt_count_) &&
	(table_size_  == filter.table_size_) &&
	(random_seed_ == filter.random_seed_)
	) {
      for (std::size_t i = 0; i < raw_table_size_; ++i) {
	bit_table_[i] &= filter.bit_table_[i];
      }
    }
    return *this;
  }

  inline bloom_filter& operator |= (const bloom_filter& filter)
  {
    /* union */
    if (
	(salt_count_  == filter.salt_count_) &&
	(table_size_  == filter.table_size_) &&
	(random_seed_ == filter.random_seed_)
	) {
      for (std::size_t i = 0; i < raw_table_size_; ++i) {
        bit_table_[i] |= filter.bit_table_[i];
      }
    }
    return *this;
  }

  inline bloom_filter& operator ^= (const bloom_filter& filter)
  {
    /* difference */
    if (
	(salt_count_  == filter.salt_count_) &&
	(table_size_  == filter.table_size_) &&
	(random_seed_ == filter.random_seed_)
	) {
      for (std::size_t i = 0; i < raw_table_size_; ++i) {
	bit_table_[i] ^= filter.bit_table_[i];
      }
    }
    return *this;
  }

  inline const cell_type* table() const
  {
    return bit_table_;
  }

protected:

  inline virtual void compute_indices(const bloom_type& hash, std::size_t& bit_index, std::size_t& bit) const
  {
    bit_index = hash % table_size_;
    bit = bit_index % bits_per_char;
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
      std::copy(predef_salt,predef_salt + predef_salt_count,std::back_inserter(salt_));
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
    t += (((t % bits_per_char) != 0) ? (bits_per_char - (t % bits_per_char)) : 0);
    *table_size = t;
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

  std::vector<bloom_type> salt_;
  unsigned char*       bit_table_;
  std::size_t         salt_count_;
  std::size_t         table_size_;
  std::size_t         raw_table_size_;
  std::size_t         inserted_element_count_;
  std::size_t         random_seed_;

public:
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<bloom_filter*>& ls);
};
WRITE_CLASS_ENCODER(bloom_filter)

inline bloom_filter operator & (const bloom_filter& a, const bloom_filter& b)
{
  bloom_filter result = a;
  result &= b;
  return result;
}

inline bloom_filter operator | (const bloom_filter& a, const bloom_filter& b)
{
  bloom_filter result = a;
  result |= b;
  return result;
}

inline bloom_filter operator ^ (const bloom_filter& a, const bloom_filter& b)
{
  bloom_filter result = a;
  result ^= b;
  return result;
}


class compressible_bloom_filter : public bloom_filter
{
public:

  compressible_bloom_filter(const std::size_t& predicted_element_count,
			    const double& false_positive_probability,
			    const std::size_t& random_seed)
    : bloom_filter(predicted_element_count,false_positive_probability,random_seed)
  {
    size_list.push_back(table_size_);
  }

  inline virtual std::size_t size() const
  {
    return size_list.back();
  }

  inline bool compress(const double& percentage)
  {
    if ((0.0 >= percentage) || (percentage >= 100.0))
    {
      return false;
    }

    std::size_t original_table_size = size_list.back();
    std::size_t new_table_size = static_cast<std::size_t>((size_list.back() * (1.0 - (percentage / 100.0))));
    new_table_size -= (((new_table_size % bits_per_char) != 0) ? (new_table_size % bits_per_char) : 0);

    if ((bits_per_char > new_table_size) || (new_table_size >= original_table_size))
    {
      return false;
    }

    cell_type* tmp = new cell_type[new_table_size / bits_per_char];
    std::copy(bit_table_, bit_table_ + (new_table_size / bits_per_char), tmp);
    cell_type* itr = bit_table_ + (new_table_size / bits_per_char);
    cell_type* end = bit_table_ + (original_table_size / bits_per_char);
    cell_type* itr_tmp = tmp;

    while (end != itr)
    {
      *(itr_tmp++) |= (*itr++);
    }

    delete[] bit_table_;
    bit_table_ = tmp;
    size_list.push_back(new_table_size);

    return true;
  }

private:

  inline virtual void compute_indices(const bloom_type& hash, std::size_t& bit_index, std::size_t& bit) const
  {
    bit_index = hash;
    for (std::size_t i = 0; i < size_list.size(); ++i)
    {
      bit_index %= size_list[i];
    }
    bit = bit_index % bits_per_char;
  }

  std::vector<std::size_t> size_list;
};

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
