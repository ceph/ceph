// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "constants.hpp"
#include "keys.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cstring>
#include <iostream>

int main()
{
  using namespace kvrgw;

  const auto tenant_counter =
      make_l_key(kLocalTypeNumeric, kLocalCounterTenantId);
  assert(tenant_counter.len == 11);
  assert(tenant_counter.data[0] == static_cast<uint8_t>(kNamespaceLocal));
  assert(tenant_counter.data[1] == static_cast<uint8_t>(kLocalTypeNumeric));

  const auto parsed = parse_l_key(tenant_counter.view());
  assert(parsed);
  assert(parsed->type == kLocalTypeNumeric);
  assert(parsed->name == "tenant_id");

  const auto id_map = make_l_key(kLocalTypeIdMap, "compression-algorithm");
  const auto id_parsed = parse_l_key(id_map.view());
  assert(id_parsed);
  assert(id_parsed->type == kLocalTypeIdMap);
  assert(id_parsed->name == "compression-algorithm");

  assert(!parse_l_key("X"));
  assert(!parse_l_key(std::string(1, kNamespaceLocal)));
  assert(!parse_l_key(std::string({kNamespaceLocal, 'X', 'a'})));

  // V: key tests
  {
    bucket_id_t bucket_id(0x0101010101010101ULL);
    std::string object_name = "photos/cat.jpg";

    auto vk1 = make_v_key(bucket_id, object_name, version_id_t{0xFFFFFFFF});
    auto vk2 = make_v_key(bucket_id, object_name, version_id_t{0xFFFFFFFE});
    auto vk3 = make_v_key(bucket_id, object_name, version_id_t{0x00000002});

    // Lower vid value → smaller BE bytes → sorts first in forward scan
    // This means most recently displaced version (lowest vid) sorts first
    assert(vk3.view() < vk2.view());
    assert(vk2.view() < vk1.view());

    // Parse round-trip
    auto p1 = parse_v_key(vk1.view());
    assert(p1);
    assert(p1->bucket_id == bucket_id);
    assert(p1->object_name == object_name);
    assert(p1->version_id == kNullVersion);

    auto p2 = parse_v_key(vk2.view());
    assert(p2);
    assert(p2->version_id == kFirstVersionId);

    auto p3 = parse_v_key(vk3.view());
    assert(p3);
    assert(p3->version_id == version_id_t{0x00000002});

    // Prefix matches
    auto prefix = make_v_prefix(bucket_id, object_name);
    assert(vk1.view().substr(0, prefix.len) == prefix.view());
    assert(vk2.view().substr(0, prefix.len) == prefix.view());

    // Different object sorts differently
    auto vk_other = make_v_key(bucket_id, "zzz", version_id_t{0xFFFFFFFF});
    assert(vk_other.view() > vk1.view());
  }

  std::cout << "l_key_test passed\n";
  return 0;
}
