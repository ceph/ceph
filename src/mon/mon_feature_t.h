// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MON_FEATURE_T_H
#define CEPH_MON_FEATURE_T_H

#include <list>
#include <ostream>

#include "include/encoding.h"
#include "common/bit_str.h" // for print_bit_str()
#include "common/ceph_releases.h"
#include "common/Formatter.h"

namespace ceph {
  namespace features {
    namespace mon {
      /**
       * Get a feature's name based on its value.
       *
       * @param b raw feature value
       *
       * @remarks
       *    Consumers should not assume this interface will never change.
       * @remarks
       *    As the number of features increase, so may the internal representation
       *    of the raw features. When this happens, this interface will change
       *    accordingly. So should consumers of this interface.
       */
      static inline const char *get_feature_name(uint64_t b);
    }
  }
}

class mon_feature_t {

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  // mon-specific features
  uint64_t features;

public:

  explicit constexpr
  mon_feature_t(const uint64_t f) : features(f) { }

  mon_feature_t() :
    features(0) { }

  constexpr
  mon_feature_t(const mon_feature_t &o) :
    features(o.features) { }

  mon_feature_t& operator&=(const mon_feature_t other) {
    features &= other.features;
    return (*this);
  }

  /**
   * Obtain raw features
   *
   * @remarks
   *    Consumers should not assume this interface will never change.
   * @remarks
   *    As the number of features increase, so may the internal representation
   *    of the raw features. When this happens, this interface will change
   *    accordingly. So should consumers of this interface.
   */
  uint64_t get_raw() const {
    return features;
  }

  constexpr
  friend mon_feature_t operator&(const mon_feature_t a,
                                 const mon_feature_t b) {
    return mon_feature_t(a.features & b.features);
  }

  mon_feature_t& operator|=(const mon_feature_t other) {
    features |= other.features;
    return (*this);
  }

  constexpr
  friend mon_feature_t operator|(const mon_feature_t a,
                                 const mon_feature_t b) {
    return mon_feature_t(a.features | b.features);
  }

  constexpr
  friend mon_feature_t operator^(const mon_feature_t a,
                                 const mon_feature_t b) {
    return mon_feature_t(a.features ^ b.features);
  }

  mon_feature_t& operator^=(const mon_feature_t other) {
    features ^= other.features;
    return (*this);
  }

  bool operator==(const mon_feature_t other) const {
    return (features == other.features);
  }

  bool operator!=(const mon_feature_t other) const {
    return (features != other.features);
  }

  bool empty() const {
    return features == 0;
  }

  /**
   * Set difference of our features in respect to @p other
   *
   * Returns all the elements in our features that are not in @p other
   *
   * @returns all the features not in @p other
   */
  mon_feature_t diff(const mon_feature_t other) const {
    return mon_feature_t((features ^ other.features) & features);
  }

  /**
   * Set intersection of our features and @p other
   *
   * Returns all the elements common to both our features and the
   * features of @p other
   *
   * @returns the features common to @p other and us
   */
  mon_feature_t intersection(const mon_feature_t other) const {
    return mon_feature_t((features & other.features));
  }

  /**
   * Checks whether we have all the features in @p other
   *
   * Returns true if we have all the features in @p other
   *
   * @returns true if we contain all the features in @p other
   * @returns false if we do not contain some of the features in @p other
   */
  bool contains_all(const mon_feature_t other) const {
    mon_feature_t d = intersection(other);
    return d == other;
  }

  /**
   * Checks whether we contain any of the features in @p other.
   *
   * @returns true if we contain any of the features in @p other
   * @returns false if we don't contain any of the features in @p other
   */
  bool contains_any(const mon_feature_t other) const {
    mon_feature_t d = intersection(other);
    return !d.empty();
  }

  void set_feature(const mon_feature_t f) {
    features |= f.features;
  }

  void unset_feature(const mon_feature_t f) {
    features &= ~(f.features);
  }

  void print(std::ostream& out) const {
    out << "[";
    print_bit_str(features, out, ceph::features::mon::get_feature_name);
    out << "]";
  }

  void print_with_value(std::ostream& out) const {
    out << "[";
    print_bit_str(features, out, ceph::features::mon::get_feature_name, true);
    out << "]";
  }

  void dump(ceph::Formatter *f, const char *sec_name = NULL) const {
    f->open_array_section((sec_name ? sec_name : "features"));
    dump_bit_str(features, f, ceph::features::mon::get_feature_name);
    f->close_section();
  }

  void dump_with_value(ceph::Formatter *f, const char *sec_name = NULL) const {
    f->open_array_section((sec_name ? sec_name : "features"));
    dump_bit_str(features, f, ceph::features::mon::get_feature_name, true);
    f->close_section();
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(HEAD_VERSION, COMPAT_VERSION, bl);
    encode(features, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(COMPAT_VERSION, p);
    decode(features, p);
    DECODE_FINISH(p);
  }

  static std::list<mon_feature_t> generate_test_instances() {
    std::list<mon_feature_t> ls;
    ls.emplace_back();
    ls.emplace_back();
    ls.back().features = 1;
    ls.emplace_back();
    ls.back().features = 2;
    return ls;
  }
};
WRITE_CLASS_ENCODER(mon_feature_t)

namespace ceph {
  namespace features {
    namespace mon {
      constexpr mon_feature_t FEATURE_KRAKEN(     (1ULL << 0));
      constexpr mon_feature_t FEATURE_LUMINOUS(   (1ULL << 1));
      constexpr mon_feature_t FEATURE_MIMIC(      (1ULL << 2));
      constexpr mon_feature_t FEATURE_OSDMAP_PRUNE (1ULL << 3);
      constexpr mon_feature_t FEATURE_NAUTILUS(    (1ULL << 4));
      constexpr mon_feature_t FEATURE_OCTOPUS(    (1ULL << 5));
      constexpr mon_feature_t FEATURE_PACIFIC(    (1ULL << 6));
      // elector pinging and CONNECTIVITY mode:
      constexpr mon_feature_t FEATURE_PINGING(    (1ULL << 7));
      constexpr mon_feature_t FEATURE_QUINCY(     (1ULL << 8));
      constexpr mon_feature_t FEATURE_REEF(       (1ULL << 9));
      constexpr mon_feature_t FEATURE_SQUID(      (1ULL << 10));
      constexpr mon_feature_t FEATURE_TENTACLE(   (1ULL << 11));
      constexpr mon_feature_t FEATURE_UMBRELLA(   (1ULL << 12));


      // Release-independent features
      constexpr mon_feature_t FEATURE_NVMEOF_BEACON_DIFF(   (1ULL << 32));

      constexpr mon_feature_t FEATURE_RESERVED(   (1ULL << 63));
      constexpr mon_feature_t FEATURE_NONE(       (0ULL));

      /**
       * All the features this monitor supports
       *
       * If there's a feature above, it should be OR'ed to this list.
       */
      constexpr mon_feature_t get_supported() {
        return (
	  FEATURE_KRAKEN |
	  FEATURE_LUMINOUS |
	  FEATURE_MIMIC |
          FEATURE_OSDMAP_PRUNE |
	  FEATURE_NAUTILUS |
	  FEATURE_OCTOPUS |
	  FEATURE_PACIFIC |
	  FEATURE_PINGING |
	  FEATURE_QUINCY |
	  FEATURE_REEF |
	  FEATURE_SQUID |
	  FEATURE_TENTACLE |
	  FEATURE_UMBRELLA |

	  // Release-independent features
	  FEATURE_NVMEOF_BEACON_DIFF |

	  FEATURE_NONE
	  );
      }
      /**
       * All the features that, once set, cannot be removed.
       *
       * Features should only be added to this list if you want to make
       * sure downgrades are not possible after a quorum supporting all
       * these features has been formed.
       *
       * Any feature in this list will be automatically set on the monmap's
       * features once all the monitors in the quorum support it.
       */
      constexpr mon_feature_t get_persistent() {
        return (
	  FEATURE_KRAKEN |
	  FEATURE_LUMINOUS |
	  FEATURE_MIMIC |
	  FEATURE_NAUTILUS |
	  FEATURE_OSDMAP_PRUNE |
	  FEATURE_OCTOPUS |
	  FEATURE_PACIFIC |
	  FEATURE_PINGING |
	  FEATURE_QUINCY |
	  FEATURE_REEF |
	  FEATURE_SQUID |
	  FEATURE_TENTACLE |
	  FEATURE_UMBRELLA |

	  // Release-independent features
	  FEATURE_NVMEOF_BEACON_DIFF |

	  FEATURE_NONE
	  );
      }

      constexpr mon_feature_t get_optional() {
        return (
          FEATURE_OSDMAP_PRUNE |
          FEATURE_NONE
          );
      }

      static inline mon_feature_t get_feature_by_name(const std::string &n);
    }
  }
}

static inline ceph_release_t infer_ceph_release_from_mon_features(mon_feature_t f)
{
  if (f.contains_all(ceph::features::mon::FEATURE_UMBRELLA)) {
    return ceph_release_t::umbrella;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_TENTACLE)) {
    return ceph_release_t::tentacle;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_SQUID)) {
    return ceph_release_t::squid;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_REEF)) {
    return ceph_release_t::reef;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_QUINCY)) {
    return ceph_release_t::quincy;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_PACIFIC)) {
    return ceph_release_t::pacific;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_OCTOPUS)) {
    return ceph_release_t::octopus;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
    return ceph_release_t::nautilus;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_MIMIC)) {
    return ceph_release_t::mimic;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_LUMINOUS)) {
    return ceph_release_t::luminous;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_KRAKEN)) {
    return ceph_release_t::kraken;
  }
  return ceph_release_t::unknown;
}

static inline const char *ceph::features::mon::get_feature_name(uint64_t b) {
  mon_feature_t f(b);

  if (f == FEATURE_KRAKEN) {
    return "kraken";
  } else if (f == FEATURE_LUMINOUS) {
    return "luminous";
  } else if (f == FEATURE_MIMIC) {
    return "mimic";
  } else if (f == FEATURE_OSDMAP_PRUNE) {
    return "osdmap-prune";
  } else if (f == FEATURE_NAUTILUS) {
    return "nautilus";
  } else if (f == FEATURE_PINGING) {
    return "elector-pinging";
  } else if (f == FEATURE_OCTOPUS) {
    return "octopus";
  } else if (f == FEATURE_PACIFIC) {
    return "pacific";
  } else if (f == FEATURE_QUINCY) {
    return "quincy";
  } else if (f == FEATURE_REEF) {
    return "reef";
  } else if (f == FEATURE_SQUID) {
    return "squid";
  } else if (f == FEATURE_TENTACLE) {
    return "tentacle";
  } else if (f == FEATURE_UMBRELLA) {
    return "umbrella";
  // Release-independent features
  } else if (f == FEATURE_NVMEOF_BEACON_DIFF) {
    return "nvmeof_beacon_diff";
  } else if (f == FEATURE_RESERVED) {
    return "reserved";
  }
  return "unknown";
}

inline mon_feature_t ceph::features::mon::get_feature_by_name(const std::string &n) {

  if (n == "kraken") {
    return FEATURE_KRAKEN;
  } else if (n == "luminous") {
    return FEATURE_LUMINOUS;
  } else if (n == "mimic") {
    return FEATURE_MIMIC;
  } else if (n == "osdmap-prune") {
    return FEATURE_OSDMAP_PRUNE;
  } else if (n == "nautilus") {
    return FEATURE_NAUTILUS;
  } else if (n == "feature-pinging") {
    return FEATURE_PINGING;
  } else if (n == "octopus") {
    return FEATURE_OCTOPUS;
  } else if (n == "pacific") {
    return FEATURE_PACIFIC;
  } else if (n == "quincy") {
    return FEATURE_QUINCY;
  } else if (n == "reef") {
    return FEATURE_REEF;
  } else if (n == "squid") {
    return FEATURE_SQUID;
  } else if (n == "tentacle") {
    return FEATURE_TENTACLE;
  } else if (n == "umbrella") {
    return FEATURE_UMBRELLA;
  // Release-independent features
  } else if (n == "nvmeof_beacon_diff") {
    return FEATURE_NVMEOF_BEACON_DIFF;
  } else if (n == "reserved") {
    return FEATURE_RESERVED;
  }
  return FEATURE_NONE;
}

inline std::ostream& operator<<(std::ostream& out, const mon_feature_t& f) {
  out << "mon_feature_t(";
  f.print(out);
  out << ")";
  return out;
}

#endif
