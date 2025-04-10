// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_THROTTLER_H
#define RBD_MIRROR_THROTTLER_H

#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#include "common/ceph_mutex.h"
#include "common/config_obs.h"
#include "include/common_fwd.h"

class Context;

namespace ceph { class Formatter; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class Throttler : public md_config_obs_t {
public:
  static Throttler *create(
      CephContext *cct,
      const std::string &config_key) {
    return new Throttler(cct, config_key);
  }
  void destroy() {
    delete this;
  }

  Throttler(CephContext *cct,
            const std::string &config_key);
  ~Throttler() override;

  void set_max_concurrent_ops(uint32_t max);
  void start_op(const std::string &ns, const std::string &id,
                Context *on_start);
  bool cancel_op(const std::string &ns, const std::string &id);
  void finish_op(const std::string &ns, const std::string &id);
  void drain(const std::string &ns, int r);

  void print_status(ceph::Formatter *f);

private:
  typedef std::pair<std::string, std::string> Id;

  CephContext *m_cct;
  const std::string m_config_key;

  ceph::mutex m_lock;
  uint32_t m_max_concurrent_ops;
  std::list<Id> m_queue;
  std::map<Id, Context *> m_queued_ops;
  std::set<Id> m_inflight_ops;

  std::vector<std::string> get_tracked_keys() const noexcept override {
    return std::vector<std::string>{m_config_key};
  }
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string> &changed) override;
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::Throttler<librbd::ImageCtx>;

#endif // RBD_MIRROR_THROTTLER_H
