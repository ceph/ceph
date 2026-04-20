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

#include <cstdint>
#include <iosfwd>
#include <list>
#include <string>

#include "include/encoding.h"

enum class ceph_release_t : std::uint8_t;
namespace ceph { class Formatter; }

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
      const char *get_feature_name(uint64_t b);
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

  void print(std::ostream& out) const;
  void print_with_value(std::ostream& out) const;

  void dump(ceph::Formatter *f, const char *sec_name = NULL) const;
  void dump_with_value(ceph::Formatter *f, const char *sec_name = NULL) const;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);

  static std::list<mon_feature_t> generate_test_instances();
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

      mon_feature_t get_feature_by_name(const std::string &n);
    }
  }
}

ceph_release_t infer_ceph_release_from_mon_features(mon_feature_t f);

std::ostream& operator<<(std::ostream& out, const mon_feature_t& f);

#endif
