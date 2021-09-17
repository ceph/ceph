// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "LRemWatchNotify.h"
#include "include/common_fwd.h"

namespace librados {

class LRemRadosClient;
class LRemWatchNotify;

class LRemCluster {
public:
  struct ObjectLocator {
    std::string nspace;
    std::string name;

    ObjectLocator(const std::string& nspace, const std::string& name)
      : nspace(nspace), name(name) {
    }

    bool operator<(const ObjectLocator& rhs) const {
      if (nspace != rhs.nspace) {
        return nspace < rhs.nspace;
      }
      return name < rhs.name;
    }
  };

  struct ObjectHandler {
    virtual ~ObjectHandler() {}

    virtual void handle_removed(LRemRadosClient* lrem_rados_client) = 0;
  };

  LRemCluster() : m_watch_notify(this) {
  }
  virtual ~LRemCluster() {
  }

  virtual LRemRadosClient *create_rados_client(CephContext *cct) = 0;

  virtual int register_object_handler(LRemRadosClient *client,
                                      int64_t pool_id,
                                      const ObjectLocator& locator,
                                      ObjectHandler* object_handler) = 0;
  virtual void unregister_object_handler(LRemRadosClient *client,
                                         int64_t pool_id,
                                         const ObjectLocator& locator,
                                         ObjectHandler* object_handler) = 0;

  LRemWatchNotify *get_watch_notify() {
    return &m_watch_notify;
  }

protected:
  LRemWatchNotify m_watch_notify;

};

} // namespace librados
