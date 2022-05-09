#include <common/config_values.h>
#include "os/ObjectStore.h"


// Functions
// - XStore__create
// - XStore__save_args
// are not part of XStore class to avoid type definition collision.
// Nothing here depends on WITH_SEASTAR define. This file can be included by crimson and classic.
// But even the declaration of XStore class requires crimson.

std::unique_ptr<ObjectStore> XStore__create(
  ceph::common::CephContext *cct,
  const ConfigValues& cf,
  const std::string& type,
  const std::string& data);

// Saves args used to start ceph_test_objectstore for initialization of the reactor.
void XStore__save_args(int argc, char** argv);

