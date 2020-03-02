// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "os/ObjectStore.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "alien_store.h"

namespace crimson::os {

struct AlienCollection final : public FuturizedCollection {

  ObjectStore::CollectionHandle collection;

  AlienCollection(ObjectStore::CollectionHandle ch)
  : FuturizedCollection(ch->cid),
    collection(ch) {}

  ~AlienCollection() {}
};

}
