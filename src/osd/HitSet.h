// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_HITSET_H
#define CEPH_OSD_HITSET_H

#include <boost/scoped_ptr.hpp>

#include "include/encoding.h"
#include "include/unordered_set.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"
#include "common/Clock.h"

/**
 * generic container for a HitSet
 *
 * Encapsulate a HitSetImpl of any type.  Expose a generic interface
 * to users and wrap the encoded object with a type so that it can be
 * safely decoded later.
 */

class HitSet {
public:
  typedef enum {
    TYPE_NONE = 0,
    TYPE_EXPLICIT_HASH = 1,
    TYPE_EXPLICIT_OBJECT = 2,
    TYPE_BLOOM = 3,
    TYPE_TEMP = 4
  } impl_type_t;

  static const char *get_type_name(impl_type_t t) {
    switch (t) {
    case TYPE_NONE: return "none";
    case TYPE_EXPLICIT_HASH: return "explicit_hash";
    case TYPE_EXPLICIT_OBJECT: return "explicit_object";
    case TYPE_BLOOM: return "bloom";
    case TYPE_TEMP: return "temp";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    if (impl)
      return get_type_name(impl->get_type());
    return get_type_name(TYPE_NONE);
  }

  /// abstract interface for a HitSet implementation
  class Impl {
  public:
    virtual impl_type_t get_type() const = 0;
    virtual bool is_full() const = 0;
    virtual void insert(const hobject_t& o) = 0;
    virtual bool contains(const hobject_t& o) const = 0;
    virtual unsigned insert_count() const = 0;
    virtual unsigned approx_unique_insert_count() const = 0;
    virtual void encode(bufferlist &bl) const = 0;
    virtual void decode(bufferlist::iterator& p) = 0;
    virtual void dump(Formatter *f) const = 0;
    virtual Impl* clone() const = 0;
    virtual void seal() {}
    virtual ~Impl() {}
  };

  boost::scoped_ptr<Impl> impl;
  bool sealed;

  class Params {
    /// create an Impl* of the given type
    bool create_impl(impl_type_t t);

  public:
    class Impl {
    public:
      virtual impl_type_t get_type() const = 0;
      virtual HitSet::Impl *get_new_impl() const = 0;
      virtual void encode(bufferlist &bl) const {}
      virtual void decode(bufferlist::iterator& p) {}
      virtual void dump(Formatter *f) const {}
      virtual void dump_stream(ostream& o) const {}
      virtual ~Impl() {}
    };

    Params()  {}
    explicit Params(Impl *i) : impl(i) {}
    virtual ~Params() {}

    boost::scoped_ptr<Params::Impl> impl;

    impl_type_t get_type() const {
      if (impl)
	return impl->get_type();
      return TYPE_NONE;
    }

    Params(const Params& o);
    const Params& operator=(const Params& o);

    void encode(bufferlist &bl) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<HitSet::Params*>& o);

    friend ostream& operator<<(ostream& out, const HitSet::Params& p);
  };

  HitSet() : impl(NULL), sealed(false) {}
  explicit HitSet(Impl *i) : impl(i), sealed(false) {}
  explicit HitSet(const HitSet::Params& params);

  HitSet(const HitSet& o) {
    sealed = o.sealed;
    if (o.impl)
      impl.reset(o.impl->clone());
    else
      impl.reset(NULL);
  }
  const HitSet& operator=(const HitSet& o) {
    sealed = o.sealed;
    if (o.impl)
      impl.reset(o.impl->clone());
    else
      impl.reset(NULL);
    return *this;
  }


  bool is_full() const {
    return impl->is_full();
  }
  /// insert a hash into the set
  void insert(const hobject_t& o) {
    impl->insert(o);
  }
  /// query whether a hash is in the set
  bool contains(const hobject_t& o) const {
    return impl->contains(o);
  }

  unsigned insert_count() const {
    return impl->insert_count();
  }
  unsigned approx_unique_insert_count() const {
    return impl->approx_unique_insert_count();
  }
  void seal() {
    assert(!sealed);
    sealed = true;
    impl->seal();
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HitSet*>& o);

private:
  void reset_to_type(impl_type_t type);
};
WRITE_CLASS_ENCODER(HitSet)
WRITE_CLASS_ENCODER(HitSet::Params)

typedef boost::shared_ptr<HitSet> HitSetRef;

ostream& operator<<(ostream& out, const HitSet::Params& p);

/**
 * explicitly enumerate hash hits in the set
 */
class ExplicitHashHitSet : public HitSet::Impl {
  uint64_t count;
  ceph::unordered_set<uint32_t> hits;
public:
  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_EXPLICIT_HASH;
    }
    HitSet::Impl *get_new_impl() const override {
      return new ExplicitHashHitSet;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  ExplicitHashHitSet() : count(0) {}
  explicit ExplicitHashHitSet(const ExplicitHashHitSet::Params *p) : count(0) {}
  ExplicitHashHitSet(const ExplicitHashHitSet &o) : count(o.count),
      hits(o.hits) {}

  HitSet::Impl *clone() const override {
    return new ExplicitHashHitSet(*this);
  }

  HitSet::impl_type_t get_type() const override {
    return HitSet::TYPE_EXPLICIT_HASH;
  }
  bool is_full() const override {
    return false;
  }
  void insert(const hobject_t& o) override {
    hits.insert(o.get_hash());
    ++count;
  }
  bool contains(const hobject_t& o) const override {
    return hits.count(o.get_hash());
  }
  unsigned insert_count() const override {
    return count;
  }
  unsigned approx_unique_insert_count() const override {
    return hits.size();
  }
  void encode(bufferlist &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(count, bl);
    encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) override {
    DECODE_START(1, bl);
    decode(count, bl);
    decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override;
  static void generate_test_instances(list<ExplicitHashHitSet*>& o) {
    o.push_back(new ExplicitHashHitSet);
    o.push_back(new ExplicitHashHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitHashHitSet)

/**
 * explicitly enumerate objects in the set
 */
class ExplicitObjectHitSet : public HitSet::Impl {
  uint64_t count;
  ceph::unordered_set<hobject_t> hits;
public:
  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_EXPLICIT_OBJECT;
    }
    HitSet::Impl *get_new_impl() const override {
      return new ExplicitObjectHitSet;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  ExplicitObjectHitSet() : count(0) {}
  explicit ExplicitObjectHitSet(const ExplicitObjectHitSet::Params *p) : count(0) {}
  ExplicitObjectHitSet(const ExplicitObjectHitSet &o) : count(o.count),
      hits(o.hits) {}

  HitSet::Impl *clone() const override {
    return new ExplicitObjectHitSet(*this);
  }

  HitSet::impl_type_t get_type() const override {
    return HitSet::TYPE_EXPLICIT_OBJECT;
  }
  bool is_full() const override {
    return false;
  }
  void insert(const hobject_t& o) override {
    hits.insert(o);
    ++count;
  }
  bool contains(const hobject_t& o) const override {
    return hits.count(o);
  }
  unsigned insert_count() const override {
    return count;
  }
  unsigned approx_unique_insert_count() const override {
    return hits.size();
  }
  void encode(bufferlist &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(count, bl);
    encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) override {
    DECODE_START(1, bl);
    decode(count, bl);
    decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override;
  static void generate_test_instances(list<ExplicitObjectHitSet*>& o) {
    o.push_back(new ExplicitObjectHitSet);
    o.push_back(new ExplicitObjectHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitObjectHitSet)

/**
 * use a bloom_filter to track hits to the set
 */
class BloomHitSet : public HitSet::Impl {
  compressible_bloom_filter bloom;

public:
  HitSet::impl_type_t get_type() const override {
    return HitSet::TYPE_BLOOM;
  }

  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_BLOOM;
    }
    HitSet::Impl *get_new_impl() const override {
      return new BloomHitSet;
    }

    uint32_t fpp_micro;    ///< false positive probability / 1M
    uint64_t target_size;  ///< number of unique insertions we expect to this HitSet
    uint64_t seed;         ///< seed to use when initializing the bloom filter

    Params()
      : fpp_micro(0), target_size(0), seed(0) {}
    Params(double fpp, uint64_t t, uint64_t s)
      : fpp_micro(fpp * 1000000.0), target_size(t), seed(s) {}
    Params(const Params &o)
      : fpp_micro(o.fpp_micro),
	target_size(o.target_size),
	seed(o.seed) {}
    ~Params() override {}

    double get_fpp() const {
      return (double)fpp_micro / 1000000.0;
    }
    void set_fpp(double f) {
      fpp_micro = (unsigned)(llrintl(f * 1000000.0));
    }

    void encode(bufferlist& bl) const override {
      ENCODE_START(1, 1, bl);
      encode(fpp_micro, bl);
      encode(target_size, bl);
      encode(seed, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& bl) override {
      DECODE_START(1, bl);
      decode(fpp_micro, bl);
      decode(target_size, bl);
      decode(seed, bl);
      DECODE_FINISH(bl);
    }
    void dump(Formatter *f) const override;
    void dump_stream(ostream& o) const override {
      o << "false_positive_probability: "
	<< get_fpp() << ", target_size: " << target_size
	<< ", seed: " << seed;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
      o.push_back(new Params);
      (*o.rbegin())->fpp_micro = 123456;
      (*o.rbegin())->target_size = 300;
      (*o.rbegin())->seed = 99;
    }
  };

  BloomHitSet() {}
  BloomHitSet(unsigned inserts, double fpp, int seed)
    : bloom(inserts, fpp, seed)
  {}
  explicit BloomHitSet(const BloomHitSet::Params *p) : bloom(p->target_size,
                                                    p->get_fpp(),
                                                    p->seed)
  {}

  BloomHitSet(const BloomHitSet &o) {
    // oh god
    bufferlist bl;
    o.encode(bl);
    bufferlist::iterator bli = bl.begin();
    this->decode(bli);
  }

  HitSet::Impl *clone() const override {
    return new BloomHitSet(*this);
  }

  bool is_full() const override {
    return bloom.is_full();
  }

  void insert(const hobject_t& o) override {
    bloom.insert(o.get_hash());
  }
  bool contains(const hobject_t& o) const override {
    return bloom.contains(o.get_hash());
  }
  unsigned insert_count() const override {
    return bloom.element_count();
  }
  unsigned approx_unique_insert_count() const override {
    return bloom.approx_unique_element_count();
  }
  void seal() override {
    // aim for a density of .5 (50% of bit set)
    double pc = bloom.density() * 2.0;
    if (pc < 1.0)
      bloom.compress(pc);
  }

  void encode(bufferlist &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(bloom, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) override {
    DECODE_START(1, bl);
    decode(bloom, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override;
  static void generate_test_instances(list<BloomHitSet*>& o) {
    o.push_back(new BloomHitSet);
    o.push_back(new BloomHitSet(10, .1, 1));
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(BloomHitSet)

/**
 * explicitly enumerate hash hits with incremental temp in the set
 */
class TempHitSet : public HitSet::Impl {
public:
  static const uint32_t TEMPMAX = UINT32_MAX * 0.9;

  template <class T>
  struct ExclusiveKeyAsHash {
      size_t operator()(T key) const noexcept{
        return key;
      }
  };

  struct temp_info {
    uint32_t temp;
    uint32_t last_decay;

    temp_info() : temp(0) {
      last_decay = ceph_clock_now().tv.tv_sec;
    }
    temp_info(uint32_t t) {
      temp = MIN(t, TEMPMAX);
      last_decay = ceph_clock_now().tv.tv_sec;
    }
    temp_info(uint32_t t, uint32_t l) : last_decay(l) {
      temp = MIN(t, TEMPMAX);
    }
    temp_info(const temp_info& ti) : last_decay(ti.last_decay) {
      temp = MIN(ti.temp, TEMPMAX);
    }

    DENC(temp_info, v, p) {
      DENC_START(1, 1, p);
      denc_varint(v.temp, p);
      denc(v.last_decay, p);
      DENC_FINISH(p);
    }
  };

  class RankHistogram {
    std::vector<int32_t> h; // savepoint histogram
    std::vector<int32_t> present; // present histogram
    uint32_t period, total;
    bool fresh_p; // if the present hist is fresh
    uint64_t last_decay_c; // last decay cycle
    uint64_t last_decay_r; // last decay remiander

    private:
      void _expand_to(unsigned s) {
	if (s > h.size())
	  h.resize(s, 0);
      }
      void _contract() {
	unsigned p = h.size();
	while (p > 1 && h[p-1] == 0)
	  --p;
	h.resize(p);
      }
    public:
      RankHistogram(uint32_t p, uint32_t time) : 
	period(p), total(0), fresh_p(false), last_decay_r(0){
	last_decay_c = time / period;
      }

      void set_dp(uint32_t dp) {
	last_decay_c = period * last_decay_c / dp;
	period = dp;
	last_decay_r = 0;
	fresh_p = false;
      }

      void clear(uint32_t time) {
	h.clear();
	present.clear();
        total = 0;
	fresh_p = false;
	last_decay_c = time / period;
	last_decay_r = 0;
      }

      template< class InputIt >
      void sync(InputIt begin, InputIt end, uint32_t time = 0) {
	clear(time);
	while(begin != end) {
	  add(*begin);
	  begin++;
	}
      }

      void add(uint32_t v) {
	unsigned bin = cbits(v);
	bin = MIN(31, bin);
	_expand_to(bin + 1);
	h[bin]++;
	total++;
	_contract();
      }

      void insert(uint32_t v) {
	unsigned bin = cbits(v);
	if (bin > 0 && bin != cbits(v-1)) {
	  _expand_to(bin + 1);
	  h[bin]++;
	  h[bin-1]--;
	  _contract();
	}
      }

      void sub(uint32_t v) {
	unsigned bin = cbits(v);
	if (total > 0 &&
      bin + 1 <= h.size() &&
      h[bin] > 0) {
	  h[bin]--;
	  total--;
	  _contract();
	}
      }

      void decay(uint64_t time) {
	int bits = time / period - last_decay_c;
	if (bits <= 0 || h.empty())
	  return;
	auto p = h.begin();
	/// Objects at a temperature of 0 don't decay
	p++;
	for ( ; p != h.end(); ++p) {
	  /// demote objs to a lower bar
	  *(p-1) += *p - (*p >> bits);
	  *p >>= bits;
	}
	last_decay_c = time / period;
	last_decay_r = 0;
	fresh_p = false;
      }

      /// generate a present histogram which is more accurate
      bool fresh_present(uint64_t time) {
	decay(time);
	uint64_t remainder = (time - last_decay_c* period) % period;
	if (h.empty() ||
	    (fresh_p && remainder - last_decay_r < period / 8))
	  return false;

	// fresh the present histogram
	present.resize(h.size());
	auto p1 = present.begin();
	auto p2 = h.begin();
	/// Objects at a temperature of 0 don't decay
	*(p1++) = *(p2++);
	for ( ; p2 != h.end(); ++p2, ++p1) {
	  uint32_t demote = remainder * (*p2) / (2 * period);
	  *(p1 - 1) += demote;
	  *p1 = *p2 - demote;
	}
	fresh_p = true;
	last_decay_r = remainder;
	return true;
      }

      uint32_t get_position_value(uint64_t m) {
        if (m >= total)
          return 0;
	int64_t target = total - m;
	uint32_t pos = 1;
	for (auto p = present.begin(); p != present.end(); ++p) {
          target -= *p;
	  if (target <= 0) {
                pos += target * MAX(pos/2 ,1) / MAX(1, *p);
                break;
          }
	  pos <<= 1;
	}
	return pos;
      }

      uint64_t get_value_position(uint32_t v) {
        int64_t lower = 0;
        uint32_t pos = 1;
        for (auto p = present.begin(); p!= present.end(); ++p) {
          if (v < pos) {
                // lower += (*p) * (v - pos / 2) / MAX(pos / 2, 1);
                break;
          }
          pos <<= 1;
          lower += *p;
      	}
        return total - lower;
      }
      uint32_t get_avg() const {
	float avg  = 0;
	int bin = 0;
	for (auto p = present.begin(); p != present.end(); ++p) {
	  avg += float(*p) / total * bin;
	  bin = (bin ? bin << 1 : 1 );
	}
	return (uint32_t)avg;
      }
  };

private:
  typedef ceph::unordered_map<
    uint32_t, temp_info, ExclusiveKeyAsHash<unsigned>
    > HitTable;

  HitTable hits;
  boost::scoped_ptr<RankHistogram> rh;

public:
  uint32_t decay_period;

public:
  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_TEMP;
    }
    HitSet::Impl *get_new_impl() const override {
      return new TempHitSet;
    }

    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  explicit TempHitSet()
    : decay_period(1024) {
    uint32_t now_sec = ceph_clock_now().tv.tv_sec;
    rh.reset(new RankHistogram(decay_period, now_sec));
  }
  explicit TempHitSet(const TempHitSet::Params *p)
    : decay_period(1024) {
    uint32_t now_sec = ceph_clock_now().tv.tv_sec;
    rh.reset(new RankHistogram(decay_period, now_sec));
  }
  TempHitSet(const TempHitSet &o)
    : hits(o.hits), decay_period(o.decay_period) {
    uint32_t now_sec = ceph_clock_now().tv.tv_sec;
    rh.reset(new RankHistogram(decay_period, now_sec));
  }

  HitSet::Impl *clone() const override {
    return new TempHitSet(*this);
  }

  HitSet::impl_type_t get_type() const override {
    return HitSet::TYPE_TEMP;
  }
  bool is_full() const override {
    return false;
  }
  void set_dp(uint32_t dp) {
    decay_period = dp;
  }
  void insert(const hobject_t &o) override {
    insert(o, false);
  }
  uint32_t insert(const hobject_t &o, bool created);
  void set_temp(const hobject_t &o, uint32_t t) {
    HitTable::iterator it = hits.find(o.get_hash());
    if (it == hits.end()) {
      hits.emplace(o.get_hash(), t);
      rh->add(t);
    } else {
      it->second.temp = t;
    }
  }
  bool contains(const hobject_t& o) const override {
    return hits.count(o.get_hash());
  }
  unsigned insert_count() const override {
    // use histogram rather than count to prevent overflow
    return rh->get_avg() * hits.size();
  }
  unsigned approx_unique_insert_count() const override {
    return hits.size();
  }
  uint32_t get_temp(const hobject_t& o) {
    HitTable::iterator it = hits.find(o.get_hash());
    if (it == hits.end())
      return 0;
    else {
      _update_temp(it->second);
      return it->second.temp;
    }
  }
  void evict(const hobject_t& o) {
    HitTable::iterator it = hits.find(o.get_hash());
    if (it == hits.end())
      return;
    rh->sub(it->second.temp);
    hits.erase(it);
  }
  uint32_t get_rank_temp(uint64_t m) {
    rh->fresh_present(ceph_clock_now().tv.tv_sec);
    return rh->get_position_value(m * approx_unique_insert_count() / 1000000);
  }
  uint64_t get_oid_rank(const hobject_t& o) {
    rh->fresh_present(ceph_clock_now().tv.tv_sec);
    uint32_t temp = get_temp(o);
    return rh->get_value_position(temp) * 1000000 / approx_unique_insert_count();
  }
  uint32_t get_avg_temp() {
    rh->fresh_present(ceph_clock_now().tv.tv_sec);
    return rh->get_avg();
  }
  // sync the temperature and rank histogram
  void sync();

  void encode(bufferlist &bl) const override {
    ENCODE_START(1, 1, bl);
    ::encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) override {
    DECODE_START(1, bl);
    ::decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override;
  static void generate_test_instances(list<TempHitSet*>& o) {
    o.push_back(new TempHitSet);
    o.push_back(new TempHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }

private:
  void _update_temp(temp_info& t) {
    uint32_t now_sec = ceph_clock_now().tv.tv_sec;
    assert(t.last_decay <= now_sec);
    assert(decay_period > 0);
    uint32_t periods = (now_sec - t.last_decay) / decay_period;
    if (periods == 0)
      t.temp = MIN(TEMPMAX, t.temp);
    else {
      t.temp >>= periods;
      t.last_decay = now_sec;
    }
  }
};
WRITE_CLASS_DENC(TempHitSet::temp_info)
WRITE_CLASS_ENCODER(TempHitSet)

#endif
