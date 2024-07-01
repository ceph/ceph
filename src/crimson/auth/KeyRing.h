// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

class KeyRing;

namespace crimson::auth {
  // see KeyRing::from_ceph_context
  seastar::future<KeyRing*> load_from_keyring(KeyRing* keyring);
  seastar::future<KeyRing*> load_from_keyfile(KeyRing* keyring);
  seastar::future<KeyRing*> load_from_key(KeyRing* keyring);
}
