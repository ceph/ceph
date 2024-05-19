// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/MirrorDaemonServiceInfo.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "include/rbd/librbd.hpp"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/Throttle.h"
#include "global/global_context.h"
#include <fstream>
#include <functional>
#include <iostream>
#include <regex>
#include <set>
#include <boost/program_options.hpp>
#include "include/ceph_assert.h"

#include <atomic>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd::action::MirrorPool: "

namespace rbd {
namespace action {
namespace mirror_pool {

namespace at = argument_types;
namespace po = boost::program_options;

static const std::string ALL_NAME("all");
static const std::string SITE_NAME("site-name");

namespace {

void add_site_name_optional(po::options_description *options) {
  options->add_options()
    (SITE_NAME.c_str(), po::value<std::string>(), "local site name");
}

int set_site_name(librados::Rados& rados, const std::string& site_name) {
  librbd::RBD rbd;
  int r = rbd.mirror_site_name_set(rados, site_name);
  if (r == -EOPNOTSUPP) {
    std::cerr << "rbd: cluster does not support site names" << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: failed to set site name" << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

struct MirrorPeerDirection {};

void validate(boost::any& v, const std::vector<std::string>& values,
              MirrorPeerDirection *target_type, int permit_tx) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  if (s == "rx-only") {
    v = boost::any(RBD_MIRROR_PEER_DIRECTION_RX);
  } else if (s == "rx-tx") {
    v = boost::any(RBD_MIRROR_PEER_DIRECTION_RX_TX);
  } else if (permit_tx != 0 && s == "tx-only") {
    v = boost::any(RBD_MIRROR_PEER_DIRECTION_TX);
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void add_direction_optional(po::options_description *options) {
  options->add_options()
    ("direction", po::value<MirrorPeerDirection>(),
     "mirroring direction (rx-only, rx-tx)\n"
     "[default: rx-tx]");
}

int validate_mirroring_enabled(librados::IoCtx& io_ctx) {
  librbd::RBD rbd;
  rbd_mirror_mode_t mirror_mode;
  int r = rbd.mirror_mode_get(io_ctx, &mirror_mode);
  if (r < 0) {
    std::cerr << "rbd: failed to retrieve mirror mode: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  if (mirror_mode == RBD_MIRROR_MODE_DISABLED) {
    std::cerr << "rbd: mirroring not enabled on the pool" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int validate_uuid(const std::string &uuid) {
  std::regex pattern("^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$",
                       std::regex::icase);
  std::smatch match;
  if (!std::regex_match(uuid, match, pattern)) {
    std::cerr << "rbd: invalid uuid '" << uuid << "'" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int read_key_file(std::string path, std::string* key) {
  std::ifstream key_file;
  key_file.open(path);
  if (key_file.fail()) {
    std::cerr << "rbd: failed to open " << path << std::endl;
    return -EINVAL;
  }

  std::getline(key_file, *key);
  if (key_file.bad()) {
    std::cerr << "rbd: failed to read key from " << path << std::endl;
    return -EINVAL;
  }

  key_file.close();
  return 0;
}

void add_uuid_option(po::options_description *positional) {
  positional->add_options()
    ("uuid", po::value<std::string>(), "peer uuid");
}

int get_uuid(const po::variables_map &vm, size_t arg_index,
             std::string *uuid) {
  *uuid = utils::get_positional_argument(vm, arg_index);
  if (uuid->empty()) {
    std::cerr << "rbd: must specify peer uuid" << std::endl;
    return -EINVAL;
  }
  return validate_uuid(*uuid);
}

int get_remote_cluster_spec(const po::variables_map &vm,
                            const std::string &spec,
                            std::string *remote_client_name,
                            std::string *remote_cluster,
                            std::map<std::string, std::string>* attributes) {
  if (vm.count("remote-client-name")) {
    *remote_client_name = vm["remote-client-name"].as<std::string>();
  }
  if (vm.count("remote-cluster")) {
    *remote_cluster = vm["remote-cluster"].as<std::string>();
  }
  if (vm.count("remote-mon-host")) {
    (*attributes)["mon_host"] = vm["remote-mon-host"].as<std::string>();
  }
  if (vm.count("remote-key-file")) {
    std::string key;
    int r = read_key_file(vm["remote-key-file"].as<std::string>(), &key);
    if (r < 0) {
      return r;
    }
    (*attributes)["key"] = key;
  }

  if (!spec.empty()) {
    std::regex pattern("^(?:(client\\.[^@]+)@)?([^/@]+)$");
    std::smatch match;
    if (!std::regex_match(spec, match, pattern)) {
      std::cerr << "rbd: invalid spec '" << spec << "'" << std::endl;
      return -EINVAL;
    }
    if (match[1].matched) {
      *remote_client_name = match[1];
    }
    *remote_cluster = match[2];
  }

  if (remote_cluster->empty()) {
    std::cerr << "rbd: remote cluster was not specified" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int set_peer_config_key(librados::IoCtx& io_ctx, const std::string& peer_uuid,
                        std::map<std::string, std::string>&& attributes) {
  librbd::RBD rbd;
  int r = rbd.mirror_peer_site_set_attributes(io_ctx, peer_uuid, attributes);
  if (r == -EPERM) {
    std::cerr << "rbd: permission denied attempting to set peer "
              << "config-key secrets in the monitor" << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: failed to update mirroring peer config: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

int get_peer_config_key(librados::IoCtx& io_ctx, const std::string& peer_uuid,
                        std::map<std::string, std::string>* attributes) {
  librbd::RBD rbd;
  int r = rbd.mirror_peer_site_get_attributes(io_ctx, peer_uuid, attributes);
  if (r == -ENOENT) {
    return r;
  } else if (r == -EPERM) {
    std::cerr << "rbd: permission denied attempting to access peer "
              << "config-key secrets from the monitor" << std::endl;
    return r;
  } else if (r == -EINVAL) {
    std::cerr << "rbd: corrupt mirroring peer config" << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: error reading mirroring peer config: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int update_peer_config_key(librados::IoCtx& io_ctx,
                           const std::string& peer_uuid,
                           const std::string& key,
                           const std::string& value) {
  std::map<std::string, std::string> attributes;
  int r = get_peer_config_key(io_ctx, peer_uuid, &attributes);
  if (r == -ENOENT) {
    return set_peer_config_key(io_ctx, peer_uuid, {{key, value}});
  } else if (r < 0) {
    return r;
  }

  if (value.empty()) {
    attributes.erase(key);
  } else {
    attributes[key] = value;
  }
  return set_peer_config_key(io_ctx, peer_uuid, std::move(attributes));
}

int format_mirror_peers(librados::IoCtx& io_ctx,
                        at::Format::Formatter formatter,
                        const std::vector<librbd::mirror_peer_site_t> &peers,
                        bool config_key) {
  if (formatter != nullptr) {
    formatter->open_array_section("peers");
  } else {
    std::cout <<  "Peer Sites: ";
    if (peers.empty()) {
      std::cout << "none";
    }
    std::cout << std::endl;
  }

  for (auto &peer : peers) {
    std::map<std::string, std::string> attributes;
    if (config_key) {
      int r = get_peer_config_key(io_ctx, peer.uuid, &attributes);
      if (r < 0 && r != -ENOENT) {
        return r;
      }
    }

    std::string direction;
    switch (peer.direction) {
    case RBD_MIRROR_PEER_DIRECTION_RX:
      direction = "rx-only";
      break;
    case RBD_MIRROR_PEER_DIRECTION_TX:
      direction = "tx-only";
      break;
    case RBD_MIRROR_PEER_DIRECTION_RX_TX:
      direction = "rx-tx";
      break;
    default:
      direction = "unknown";
      break;
    }

    if (formatter != nullptr) {
      formatter->open_object_section("peer");
      formatter->dump_string("uuid", peer.uuid);
      formatter->dump_string("direction", direction);
      formatter->dump_string("site_name", peer.site_name);
      formatter->dump_string("mirror_uuid", peer.mirror_uuid);
      formatter->dump_string("client_name", peer.client_name);
      for (auto& pair : attributes) {
        formatter->dump_string(pair.first.c_str(), pair.second);
      }
      formatter->close_section();
    } else {
      std::cout << std::endl
                << "UUID: " << peer.uuid << std::endl
                << "Name: " << peer.site_name << std::endl;
      if (peer.direction != RBD_MIRROR_PEER_DIRECTION_RX ||
          !peer.mirror_uuid.empty()) {
        std::cout << "Mirror UUID: " << peer.mirror_uuid << std::endl;
      }
      std::cout << "Direction: " << direction << std::endl;
      if (peer.direction != RBD_MIRROR_PEER_DIRECTION_TX ||
          !peer.client_name.empty()) {
        std::cout << "Client: " << peer.client_name << std::endl;
      }
      if (config_key) {
        std::cout << "Mon Host: " << attributes["mon_host"] << std::endl
                  << "Key: " << attributes["key"] << std::endl;
      }
      if (peer.site_name != peers.rbegin()->site_name) {
        std::cout << std::endl;
      }
    }
  }

  if (formatter != nullptr) {
    formatter->close_section();
  }
  return 0;
}

class ImageRequestBase {
public:
  void send() {
    dout(20) << this << " " << __func__ << ": image_name=" << m_image_name
             << dendl;

    auto ctx = new LambdaContext([this](int r) {
        handle_finalize(r);
      });

    // will pause here until slots are available
    m_finalize_ctx = m_throttle.start_op(ctx);

    open_image();
  }

protected:
  ImageRequestBase(librados::IoCtx &io_ctx, OrderedThrottle &throttle,
                   const std::string &image_name)
    : m_io_ctx(io_ctx), m_throttle(throttle), m_image_name(image_name) {
  }
  virtual ~ImageRequestBase() {
  }

  virtual bool skip_get_info() const {
    return false;
  }
  virtual void get_info(librbd::Image &image, librbd::mirror_image_info_t *info,
                        librbd::RBD::AioCompletion *aio_comp) {
    image.aio_mirror_image_get_info(info, sizeof(librbd::mirror_image_info_t),
                                    aio_comp);
  }

  virtual bool skip_action(const librbd::mirror_image_info_t &info) const {
    return false;
  }
  virtual void execute_action(librbd::Image &image,
                              librbd::RBD::AioCompletion *aio_comp) = 0;
  virtual void handle_execute_action(int r) {
    dout(20) << this << " " << __func__ << ": r=" << r << dendl;

    if (r < 0 && r != -ENOENT) {
      std::cerr << "rbd: failed to " << get_action_type() << " image "
                << m_image_name << ": " << cpp_strerror(r) << std::endl;
      m_ret_val = r;
    }

    close_image();
  }

  virtual void finalize_action() {
  }
  virtual std::string get_action_type() const = 0;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN_IMAGE
   *    |
   *    v
   * GET_INFO
   *    |
   *    v
   * EXECUTE_ACTION
   *    |
   *    v
   * CLOSE_IMAGE
   *    |
   *    v
   * FINALIZE_ACTION
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  OrderedThrottle &m_throttle;
  const std::string m_image_name;

  librbd::Image m_image;
  Context *m_finalize_ctx = nullptr;

  librbd::mirror_image_info_t m_mirror_image_info;

  int m_ret_val = 0;

  void open_image() {
    dout(20) << this << " " << __func__ << dendl;

    librbd::RBD rbd;
    auto aio_completion = utils::create_aio_completion<
      ImageRequestBase, &ImageRequestBase::handle_open_image>(this);
    rbd.aio_open(m_io_ctx, m_image, m_image_name.c_str(), nullptr,
                 aio_completion);
  }

  void handle_open_image(int r) {
    dout(20) << this << " " << __func__ << ": r=" << r << dendl;

    if (r < 0) {
      std::cerr << "rbd: failed to open image "
                << m_image_name << ": " << cpp_strerror(r) << std::endl;
      m_finalize_ctx->complete(r);
      return;
    }

    get_info();
  }

  void get_info() {
    if (skip_get_info()) {
      execute_action();
      return;
    }
    dout(20) << this << " " << __func__ << dendl;

    auto aio_completion = utils::create_aio_completion<
      ImageRequestBase, &ImageRequestBase::handle_get_info>(this);
    get_info(m_image, &m_mirror_image_info, aio_completion);
  }

  void handle_get_info(int r) {
    dout(20) << this << " " << __func__ << ": r=" << r << dendl;

    if (r == -ENOENT) {
      close_image();
      return;
    } else if (r < 0) {
      std::cerr << "rbd: failed to retrieve mirror image info for "
                << m_image_name << ": " << cpp_strerror(r) << std::endl;
      m_ret_val = r;
      close_image();
      return;
    }

    execute_action();
  }

  void execute_action() {
    if (skip_action(m_mirror_image_info)) {
      close_image();
      return;
    }
    dout(20) << this << " " << __func__ << dendl;

    auto aio_completion = utils::create_aio_completion<
      ImageRequestBase, &ImageRequestBase::handle_execute_action>(this);
    execute_action(m_image, aio_completion);
  }

  void close_image() {
    dout(20) << this << " " << __func__ << dendl;

    auto aio_completion = utils::create_aio_completion<
      ImageRequestBase, &ImageRequestBase::handle_close_image>(this);
    m_image.aio_close(aio_completion);
  }

  void handle_close_image(int r) {
    dout(20) << this << " " << __func__ << ": r=" << r << dendl;

    if (r < 0) {
      std::cerr << "rbd: failed to close image "
                << m_image_name << ": " << cpp_strerror(r) << std::endl;
    }

    m_finalize_ctx->complete(r);
  }

  void handle_finalize(int r) {
    dout(20) << this << " " << __func__ << ": r=" << r << dendl;

    if (r == 0 && m_ret_val < 0) {
      r = m_ret_val;
    }
    if (r >= 0) {
      finalize_action();
    }
    m_throttle.end_op(r);
    delete this;
  }

};

class PromoteImageRequest : public ImageRequestBase {
public:
  PromoteImageRequest(librados::IoCtx &io_ctx, OrderedThrottle &throttle,
                      const std::string &image_name, std::atomic<unsigned> *counter,
                      bool force)
    : ImageRequestBase(io_ctx, throttle, image_name), m_counter(counter),
      m_force(force) {
  }

protected:
  bool skip_action(const librbd::mirror_image_info_t &info) const override {
    return (info.state != RBD_MIRROR_IMAGE_ENABLED || info.primary);
  }

  void execute_action(librbd::Image &image,
                      librbd::RBD::AioCompletion *aio_comp) override {
    image.aio_mirror_image_promote(m_force, aio_comp);
  }

  void handle_execute_action(int r) override {
    if (r >= 0) {
      (*m_counter)++;
    }
    ImageRequestBase::handle_execute_action(r);
  }

  std::string get_action_type() const override {
    return "promote";
  }

private:
  std::atomic<unsigned> *m_counter = nullptr;
  bool m_force;
};

class DemoteImageRequest : public ImageRequestBase {
public:
  DemoteImageRequest(librados::IoCtx &io_ctx, OrderedThrottle &throttle,
                     const std::string &image_name, std::atomic<unsigned> *counter)
    : ImageRequestBase(io_ctx, throttle, image_name), m_counter(counter) {
  }

protected:
  bool skip_action(const librbd::mirror_image_info_t &info) const override {
    return (info.state != RBD_MIRROR_IMAGE_ENABLED || !info.primary);
  }

  void execute_action(librbd::Image &image,
                      librbd::RBD::AioCompletion *aio_comp) override {
    image.aio_mirror_image_demote(aio_comp);
  }
  void handle_execute_action(int r) override {
    if (r >= 0) {
      (*m_counter)++;
    }
    ImageRequestBase::handle_execute_action(r);
  }

  std::string get_action_type() const override {
    return "demote";
  }

private:
  std::atomic<unsigned> *m_counter = nullptr;
};

class StatusImageRequest : public ImageRequestBase {
public:
  StatusImageRequest(
      librados::IoCtx &io_ctx, OrderedThrottle &throttle,
      const std::string &image_name,
      const std::map<std::string, std::string> &instance_ids,
      const std::vector<librbd::mirror_peer_site_t>& mirror_peers,
      const std::map<std::string, std::string> &peer_mirror_uuids_to_name,
      const MirrorDaemonServiceInfo &daemon_service_info,
      at::Format::Formatter formatter)
    : ImageRequestBase(io_ctx, throttle, image_name),
      m_instance_ids(instance_ids), m_mirror_peers(mirror_peers),
      m_peer_mirror_uuids_to_name(peer_mirror_uuids_to_name),
      m_daemon_service_info(daemon_service_info), m_formatter(formatter) {
  }

protected:
  bool skip_get_info() const override {
    return true;
  }

  void execute_action(librbd::Image &image,
                      librbd::RBD::AioCompletion *aio_comp) override {
    image.get_id(&m_image_id);
    image.aio_mirror_image_get_global_status(
      &m_mirror_image_global_status, sizeof(m_mirror_image_global_status),
      aio_comp);
  }

  void finalize_action() override {
    if (m_mirror_image_global_status.info.global_id.empty()) {
      return;
    }

    utils::populate_unknown_mirror_image_site_statuses(
      m_mirror_peers, &m_mirror_image_global_status);

    librbd::mirror_image_site_status_t local_status;
    int local_site_r = utils::get_local_mirror_image_status(
      m_mirror_image_global_status, &local_status);
    m_mirror_image_global_status.site_statuses.erase(
      std::remove_if(m_mirror_image_global_status.site_statuses.begin(),
                     m_mirror_image_global_status.site_statuses.end(),
                     [](auto& status) {
          return (status.mirror_uuid ==
                    RBD_MIRROR_IMAGE_STATUS_LOCAL_MIRROR_UUID);
        }),
      m_mirror_image_global_status.site_statuses.end());

    std::string instance_id = (local_site_r >= 0 && local_status.up &&
                               m_instance_ids.count(m_image_id)) ?
        m_instance_ids.find(m_image_id)->second : "";

    auto mirror_service = m_daemon_service_info.get_by_instance_id(instance_id);
    if (m_formatter != nullptr) {
      m_formatter->open_object_section("image");
      m_formatter->dump_string("name", m_mirror_image_global_status.name);
      m_formatter->dump_string(
        "global_id", m_mirror_image_global_status.info.global_id);
      if (local_site_r >= 0) {
        m_formatter->dump_string("state", utils::mirror_image_site_status_state(
          local_status));
        m_formatter->dump_string("description", local_status.description);
        if (mirror_service != nullptr) {
          mirror_service->dump_image(m_formatter);
        }
        m_formatter->dump_string("last_update", utils::timestr(
          local_status.last_update));
      }
      if (!m_mirror_image_global_status.site_statuses.empty()) {
        m_formatter->open_array_section("peer_sites");
        for (auto& status : m_mirror_image_global_status.site_statuses) {
          m_formatter->open_object_section("peer_site");

          auto name_it = m_peer_mirror_uuids_to_name.find(status.mirror_uuid);
          m_formatter->dump_string("site_name",
            (name_it != m_peer_mirror_uuids_to_name.end() ?
               name_it->second : ""));
          m_formatter->dump_string("mirror_uuids", status.mirror_uuid);

          m_formatter->dump_string(
            "state", utils::mirror_image_site_status_state(status));
          m_formatter->dump_string("description", status.description);
          m_formatter->dump_string("last_update", utils::timestr(
            status.last_update));
          m_formatter->close_section(); // peer_site
        }
        m_formatter->close_section(); // peer_sites
      }
      m_formatter->close_section(); // image
    } else {
      std::cout << std::endl
                << m_mirror_image_global_status.name << ":" << std::endl
  	        << "  global_id:   "
                << m_mirror_image_global_status.info.global_id << std::endl;
      if (local_site_r >= 0) {
        std::cout << "  state:       " << utils::mirror_image_site_status_state(
                    local_status) << std::endl
                  << "  description: " << local_status.description << std::endl;
        if (mirror_service != nullptr) {
          std::cout << "  service:     " <<
            mirror_service->get_image_description() << std::endl;
        }
        std::cout << "  last_update: " << utils::timestr(
          local_status.last_update) << std::endl;
      }
      if (!m_mirror_image_global_status.site_statuses.empty()) {
        std::cout << "  peer_sites:" << std::endl;
        bool first_site = true;
        for (auto& site : m_mirror_image_global_status.site_statuses) {
          if (!first_site) {
            std::cout << std::endl;
          }
          first_site = false;

          auto name_it = m_peer_mirror_uuids_to_name.find(site.mirror_uuid);
          std::cout << "    name: "
                    << (name_it != m_peer_mirror_uuids_to_name.end() ?
                          name_it->second : site.mirror_uuid)
                    << std::endl
                    << "    state: " << utils::mirror_image_site_status_state(
                      site) << std::endl
                    << "    description: " << site.description << std::endl
                    << "    last_update: " << utils::timestr(
                      site.last_update) << std::endl;
        }
      }
    }
  }

  std::string get_action_type() const override {
    return "status";
  }

private:
  const std::map<std::string, std::string> &m_instance_ids;
  const std::vector<librbd::mirror_peer_site_t> &m_mirror_peers;
  const std::map<std::string, std::string> &m_peer_mirror_uuids_to_name;
  const MirrorDaemonServiceInfo &m_daemon_service_info;
  at::Format::Formatter m_formatter;
  std::string m_image_id;
  librbd::mirror_image_global_status_t m_mirror_image_global_status;
};

template <typename RequestT>
class ImageRequestAllocator {
public:
  template <class... Args>
  RequestT *operator()(librados::IoCtx &io_ctx, OrderedThrottle &throttle,
                       const std::string &image_name, Args&&... args) {
    return new RequestT(io_ctx, throttle, image_name,
                        std::forward<Args>(args)...);
  }
};

template <typename RequestT>
class ImageRequestGenerator {
public:
  template <class... Args>
  ImageRequestGenerator(librados::IoCtx &io_ctx, Args&&... args)
    : m_io_ctx(io_ctx),
      m_factory(std::bind(ImageRequestAllocator<RequestT>(),
                          std::ref(m_io_ctx), std::ref(m_throttle),
                          std::placeholders::_1, std::forward<Args>(args)...)),
      m_throttle(g_conf().get_val<uint64_t>("rbd_concurrent_management_ops"),
                 true) {
  }

  int execute() {
    // use the alphabetical list of image names for pool-level
    // mirror image operations
    librbd::RBD rbd;
    int r = rbd.list2(m_io_ctx, &m_images);
    if (r < 0 && r != -ENOENT) {
      std::cerr << "rbd: failed to list images within pool" << std::endl;
      return r;
    }

    for (auto &image : m_images) {
      auto request = m_factory(image.name);
      request->send();
    }

    return m_throttle.wait_for_ret();
  }
private:
  typedef std::function<RequestT*(const std::string&)>  Factory;

  librados::IoCtx &m_io_ctx;
  Factory m_factory;

  OrderedThrottle m_throttle;

  std::vector<librbd::image_spec_t> m_images;

};

int get_mirror_image_status(
    librados::IoCtx& io_ctx, uint32_t* total_images,
    std::map<librbd::mirror_image_status_state_t, int>* mirror_image_states,
    MirrorHealth* mirror_image_health) {
  librbd::RBD rbd;
  int r = rbd.mirror_image_status_summary(io_ctx, mirror_image_states);
  if (r < 0) {
    std::cerr << "rbd: failed to get status summary for mirrored images: "
	      << cpp_strerror(r) << std::endl;
    return r;
  }

  *mirror_image_health = MIRROR_HEALTH_OK;
  for (auto &it : *mirror_image_states) {
    auto &state = it.first;
    if (*mirror_image_health < MIRROR_HEALTH_WARNING &&
	(state != MIRROR_IMAGE_STATUS_STATE_REPLAYING &&
	 state != MIRROR_IMAGE_STATUS_STATE_STOPPED)) {
      *mirror_image_health = MIRROR_HEALTH_WARNING;
    }
    if (*mirror_image_health < MIRROR_HEALTH_ERROR &&
	state == MIRROR_IMAGE_STATUS_STATE_ERROR) {
      *mirror_image_health = MIRROR_HEALTH_ERROR;
    }
    *total_images += it.second;
  }

  return 0;
}

} // anonymous namespace

void get_peer_bootstrap_create_arguments(po::options_description *positional,
                                         po::options_description *options) {
  at::add_pool_options(positional, options, false);
  add_site_name_optional(options);
}

int execute_peer_bootstrap_create(
    const po::variables_map &vm,
    const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              nullptr, &arg_index);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  if (vm.count(SITE_NAME)) {
    r = set_site_name(rados, vm[SITE_NAME].as<std::string>());
    if (r < 0) {
      return r;
    }
  }

  librbd::RBD rbd;
  std::string token;
  r = rbd.mirror_peer_bootstrap_create(io_ctx, &token);
  if (r == -EEXIST) {
    std::cerr << "rbd: mismatch with pre-existing RBD mirroring peer user caps"
              << std::endl;
  } else if (r < 0) {
    std::cerr << "rbd: failed to create mirroring bootstrap token: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  std::cout << token << std::endl;
  return 0;
}

void get_peer_bootstrap_import_arguments(po::options_description *positional,
                                         po::options_description *options) {
  at::add_pool_options(positional, options, false);
  add_site_name_optional(options);
  positional->add_options()
    ("token-path", po::value<std::string>(),
     "bootstrap token file (or '-' for stdin)");
  options->add_options()
    ("token-path", po::value<std::string>(),
     "bootstrap token file (or '-' for stdin)");
  add_direction_optional(options);
}

int execute_peer_bootstrap_import(
    const po::variables_map &vm,
    const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              nullptr, &arg_index);
  if (r < 0) {
    return r;
  }

  std::string token_path;
  if (vm.count("token-path")) {
    token_path = vm["token-path"].as<std::string>();
  } else {
    token_path = utils::get_positional_argument(vm, arg_index++);
  }

  if (token_path.empty()) {
    std::cerr << "rbd: token path was not specified" << std::endl;
    return -EINVAL;
  }

  rbd_mirror_peer_direction_t mirror_peer_direction =
    RBD_MIRROR_PEER_DIRECTION_RX_TX;
  if (vm.count("direction")) {
    mirror_peer_direction = vm["direction"].as<rbd_mirror_peer_direction_t>();
  }

  int fd = STDIN_FILENO;
  if (token_path != "-") {
    fd = open(token_path.c_str(), O_RDONLY|O_BINARY);
    if (fd < 0) {
      r = -errno;
      std::cerr << "rbd: error opening " << token_path << std::endl;
      return r;
    }
  }

  char token[1024];
  memset(token, 0, sizeof(token));
  r = safe_read(fd, token, sizeof(token) - 1);
  if (fd != STDIN_FILENO) {
    VOID_TEMP_FAILURE_RETRY(close(fd));
  }

  if (r < 0) {
    std::cerr << "rbd: error reading token file: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  if (vm.count(SITE_NAME)) {
    r = set_site_name(rados, vm[SITE_NAME].as<std::string>());
    if (r < 0) {
      return r;
    }
  }

  librbd::RBD rbd;
  r = rbd.mirror_peer_bootstrap_import(io_ctx, mirror_peer_direction, token);
  if (r == -ENOSYS) {
    std::cerr << "rbd: mirroring is not enabled on remote peer" << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: failed to import peer bootstrap token" << std::endl;
    return r;
  }

  return 0;
}

void get_peer_add_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_pool_options(positional, options, false);
  positional->add_options()
    ("remote-cluster-spec", "remote cluster spec\n"
     "(example: [<client name>@]<cluster name>)");
  options->add_options()
    ("remote-client-name", po::value<std::string>(), "remote client name")
    ("remote-cluster", po::value<std::string>(), "remote cluster name")
    ("remote-mon-host", po::value<std::string>(), "remote mon host(s)")
    ("remote-key-file", po::value<std::string>(),
     "path to file containing remote key");
  add_direction_optional(options);
}

int execute_peer_add(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              nullptr, &arg_index);
  if (r < 0) {
    return r;
  }

  std::string remote_client_name = g_ceph_context->_conf->name.to_str();
  std::string remote_cluster;
  std::map<std::string, std::string> attributes;
  r = get_remote_cluster_spec(
    vm, utils::get_positional_argument(vm, arg_index),
    &remote_client_name, &remote_cluster, &attributes);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  // TODO: temporary restriction to prevent adding multiple peers
  // until rbd-mirror daemon can properly handle the scenario
  librbd::RBD rbd;
  std::vector<librbd::mirror_peer_site_t> mirror_peers;
  r = rbd.mirror_peer_site_list(io_ctx, &mirror_peers);
  if (r < 0) {
    std::cerr << "rbd: failed to list mirror peers" << std::endl;
    return r;
  }

  // ignore tx-only peers since the restriction is for rx
  mirror_peers.erase(
    std::remove_if(
      mirror_peers.begin(), mirror_peers.end(),
      [](const librbd::mirror_peer_site_t& peer) {
        return (peer.direction == RBD_MIRROR_PEER_DIRECTION_TX);
      }),
    mirror_peers.end());

  if (!mirror_peers.empty()) {
    std::cerr << "rbd: multiple RX peers are not currently supported"
              << std::endl;
    return -EINVAL;
  }

  rbd_mirror_peer_direction_t mirror_peer_direction =
    RBD_MIRROR_PEER_DIRECTION_RX_TX;
  if (vm.count("direction")) {
    mirror_peer_direction = vm["direction"].as<rbd_mirror_peer_direction_t>();
  }

  std::string uuid;
  r = rbd.mirror_peer_site_add(
    io_ctx, &uuid, mirror_peer_direction, remote_cluster, remote_client_name);
  if (r == -EEXIST) {
    std::cerr << "rbd: mirror peer already exists" << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: error adding mirror peer" << std::endl;
    return r;
  }

  if (!attributes.empty()) {
    r = set_peer_config_key(io_ctx, uuid, std::move(attributes));
    if (r < 0) {
      return r;
    }
  }

  std::cout << uuid << std::endl;
  return 0;
}

void get_peer_remove_arguments(po::options_description *positional,
                               po::options_description *options) {
  at::add_pool_options(positional, options, false);
  add_uuid_option(positional);
}

int execute_peer_remove(const po::variables_map &vm,
                        const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              nullptr, &arg_index);
  if (r < 0) {
    return r;
  }

  std::string uuid;
  r = get_uuid(vm, arg_index, &uuid);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.mirror_peer_site_remove(io_ctx, uuid);
  if (r < 0) {
    std::cerr << "rbd: error removing mirror peer" << std::endl;
    return r;
  }
  return 0;
}

void get_peer_set_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_pool_options(positional, options, false);
  add_uuid_option(positional);
  positional->add_options()
    ("key", "peer parameter\n"
            "(direction, site-name, client, mon-host, key-file)")
    ("value", "new value for specified key\n"
              "(rx-only, tx-only, or rx-tx for direction)");
}

int execute_peer_set(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              nullptr, &arg_index);
  if (r < 0) {
    return r;
  }

  std::string uuid;
  r = get_uuid(vm, arg_index++, &uuid);
  if (r < 0) {
    return r;
  }

  std::set<std::string> valid_keys{{"direction", "site-name", "cluster",
                                    "client", "mon-host", "key-file"}};
  std::string key = utils::get_positional_argument(vm, arg_index++);
  if (valid_keys.find(key) == valid_keys.end()) {
    std::cerr << "rbd: must specify ";
    for (auto& valid_key : valid_keys) {
      std::cerr << "'" << valid_key << "'";
      if (&valid_key != &(*valid_keys.rbegin())) {
        std::cerr << ", ";
      }
    }
    std::cerr <<  " key." << std::endl;
    return -EINVAL;
  }

  std::string value = utils::get_positional_argument(vm, arg_index++);
  if (value.empty() && (key == "client" || key == "cluster")) {
    std::cerr << "rbd: must specify new " << key << " value." << std::endl;
  } else if (key == "key-file") {
    key = "key";
    r = read_key_file(value, &value);
    if (r < 0) {
      return r;
    }
  } else if (key == "mon-host") {
    key = "mon_host";
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  if (key == "client") {
    r = rbd.mirror_peer_site_set_client_name(io_ctx, uuid.c_str(),
                                             value.c_str());
  } else if (key == "site-name" || key == "cluster") {
    r = rbd.mirror_peer_site_set_name(io_ctx, uuid.c_str(), value.c_str());
  } else if (key == "direction") {
    MirrorPeerDirection tag;
    boost::any direction;
    try {
      validate(direction, {value}, &tag, 1);
    } catch (...) {
      std::cerr << "rbd: invalid direction" << std::endl;
      return -EINVAL;
    }

    auto peer_direction = boost::any_cast<rbd_mirror_peer_direction_t>(
      direction);
    if (peer_direction != RBD_MIRROR_PEER_DIRECTION_TX) {
      // TODO: temporary restriction to prevent adding multiple peers
      // until rbd-mirror daemon can properly handle the scenario
      std::vector<librbd::mirror_peer_site_t> mirror_peers;
      r = rbd.mirror_peer_site_list(io_ctx, &mirror_peers);
      if (r < 0) {
        std::cerr << "rbd: failed to list mirror peers" << std::endl;
        return r;
      }

      // ignore peer to be updated and tx-only peers since the restriction is
      // for rx
      mirror_peers.erase(
        std::remove_if(
          mirror_peers.begin(), mirror_peers.end(),
          [uuid](const librbd::mirror_peer_site_t& peer) {
            return (peer.uuid == uuid ||
                    peer.direction == RBD_MIRROR_PEER_DIRECTION_TX);
          }),
        mirror_peers.end());

      if (!mirror_peers.empty()) {
        std::cerr << "rbd: multiple RX peers are not currently supported"
                  << std::endl;
        return -EINVAL;
      }
    }

    r = rbd.mirror_peer_site_set_direction(io_ctx, uuid, peer_direction);
  } else {
    r = update_peer_config_key(io_ctx, uuid, key, value);
  }

  if (r  == -ENOENT) {
    std::cerr << "rbd: mirror peer " << uuid << " does not exist"
              << std::endl;
  }

  if (r < 0) {
    return r;
  }
  return 0;
}

void get_disable_arguments(po::options_description *positional,
                           po::options_description *options) {
  at::add_pool_options(positional, options, true);
}

void get_enable_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_pool_options(positional, options, true);
  positional->add_options()
    ("mode", "mirror mode [image or pool]");
  add_site_name_optional(options);
}

int execute_enable_disable(librados::IoCtx& io_ctx,
                           rbd_mirror_mode_t next_mirror_mode,
                           const std::string &mode, bool ignore_no_update) {
  librbd::RBD rbd;
  rbd_mirror_mode_t current_mirror_mode;
  int r = rbd.mirror_mode_get(io_ctx, &current_mirror_mode);
  if (r < 0) {
    std::cerr << "rbd: failed to retrieve mirror mode: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  if (current_mirror_mode == next_mirror_mode) {
    if (!ignore_no_update) {
      if (mode == "disabled") {
        std::cout << "rbd: mirroring is already " << mode << std::endl;
      } else {
        std::cout << "rbd: mirroring is already configured for "
                  << mode << " mode" << std::endl;
      }
    }
    return 0;
  } else if (next_mirror_mode == RBD_MIRROR_MODE_IMAGE &&
             current_mirror_mode == RBD_MIRROR_MODE_POOL) {
    std::cout << "note: changing mirroring mode from pool to image"
              << std::endl;
  } else if (next_mirror_mode == RBD_MIRROR_MODE_POOL &&
             current_mirror_mode == RBD_MIRROR_MODE_IMAGE) {
    std::cout << "note: changing mirroring mode from image to pool"
              << std::endl;
  }

  r = rbd.mirror_mode_set(io_ctx, next_mirror_mode);
  if (r < 0) {
    return r;
  }
  return 0;
}

int execute_disable(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  return execute_enable_disable(io_ctx, RBD_MIRROR_MODE_DISABLED, "disabled",
                                false);
}

int execute_enable(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  rbd_mirror_mode_t mirror_mode;
  std::string mode = utils::get_positional_argument(vm, arg_index++);
  if (mode == "image") {
    mirror_mode = RBD_MIRROR_MODE_IMAGE;
  } else if (mode == "pool") {
    mirror_mode = RBD_MIRROR_MODE_POOL;
  } else {
    std::cerr << "rbd: must specify 'image' or 'pool' mode." << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  bool updated = false;
  if (vm.count(SITE_NAME)) {
    librbd::RBD rbd;

    auto site_name = vm[SITE_NAME].as<std::string>();
    std::string original_site_name;
    r = rbd.mirror_site_name_get(rados, &original_site_name);
    updated = (r >= 0 && site_name != original_site_name);

    r = set_site_name(rados, site_name);
    if (r < 0) {
      return r;
    }
  }

  return execute_enable_disable(io_ctx, mirror_mode, mode, updated);
}

void get_info_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options, true);
  at::add_format_options(options);
  options->add_options()
    (ALL_NAME.c_str(), po::bool_switch(), "list all attributes");
}

int execute_info(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, false, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  rbd_mirror_mode_t mirror_mode;
  r = rbd.mirror_mode_get(io_ctx, &mirror_mode);
  if (r < 0) {
    return r;
  }

  std::string site_name;
  r = rbd.mirror_site_name_get(rados, &site_name);
  if (r < 0 && r != -EOPNOTSUPP) {
    return r;
  }

  std::vector<librbd::mirror_peer_site_t> mirror_peers;
  if (namespace_name.empty()) {
    r = rbd.mirror_peer_site_list(io_ctx, &mirror_peers);
    if (r < 0) {
      return r;
    }
  }

  std::string mirror_mode_desc;
  switch (mirror_mode) {
  case RBD_MIRROR_MODE_DISABLED:
    mirror_mode_desc = "disabled";
    break;
  case RBD_MIRROR_MODE_IMAGE:
    mirror_mode_desc = "image";
    break;
  case RBD_MIRROR_MODE_POOL:
    mirror_mode_desc = "pool";
    break;
  default:
    mirror_mode_desc = "unknown";
    break;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("mirror");
    formatter->dump_string("mode", mirror_mode_desc);
  } else {
    std::cout << "Mode: " << mirror_mode_desc << std::endl;
  }

  if (mirror_mode != RBD_MIRROR_MODE_DISABLED && namespace_name.empty()) {
    if (formatter != nullptr) {
      formatter->dump_string("site_name", site_name);
    } else {
      std::cout << "Site Name: " << site_name << std::endl
                << std::endl;
    }

    r = format_mirror_peers(io_ctx, formatter, mirror_peers,
                            vm[ALL_NAME].as<bool>());
    if (r < 0) {
      return r;
    }
  }
  if (formatter != nullptr) {
    formatter->close_section();
    formatter->flush(std::cout);
  }
  return 0;
}

void get_status_arguments(po::options_description *positional,
			  po::options_description *options) {
  at::add_pool_options(positional, options, true);
  at::add_format_options(options);
  at::add_verbose_option(options);
}

int execute_status(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, false, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  bool verbose = vm[at::VERBOSE].as<bool>();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;

  uint32_t total_images = 0;
  std::map<librbd::mirror_image_status_state_t, int> mirror_image_states;
  MirrorHealth mirror_image_health = MIRROR_HEALTH_UNKNOWN;
  r = get_mirror_image_status(io_ctx, &total_images, &mirror_image_states,
                              &mirror_image_health);
  if (r < 0) {
    return r;
  }

  MirrorDaemonServiceInfo daemon_service_info(io_ctx);
  daemon_service_info.init();

  MirrorHealth mirror_daemon_health = daemon_service_info.get_daemon_health();
  auto mirror_services = daemon_service_info.get_mirror_services();

  auto mirror_health = std::max(mirror_image_health, mirror_daemon_health);

  if (formatter != nullptr) {
    formatter->open_object_section("status");
    formatter->open_object_section("summary");
    formatter->dump_stream("health") << mirror_health;
    formatter->dump_stream("daemon_health") << mirror_daemon_health;
    formatter->dump_stream("image_health") << mirror_image_health;
    formatter->open_object_section("states");
    for (auto &it : mirror_image_states) {
      std::string state_name = utils::mirror_image_status_state(it.first);
      formatter->dump_int(state_name.c_str(), it.second);
    }
    formatter->close_section(); // states
    formatter->close_section(); // summary
  } else {
    std::cout << "health: " << mirror_health << std::endl;
    std::cout << "daemon health: " << mirror_daemon_health << std::endl;
    std::cout << "image health: " << mirror_image_health << std::endl;
    std::cout << "images: " << total_images << " total" << std::endl;
    for (auto &it : mirror_image_states) {
      std::cout << "    " << it.second << " "
		<< utils::mirror_image_status_state(it.first) << std::endl;
    }
  }

  int ret = 0;

  if (verbose) {
    // dump per-daemon status
    if (formatter != nullptr) {
      formatter->open_array_section("daemons");
      for (auto& mirror_service : mirror_services) {
        formatter->open_object_section("daemon");
        formatter->dump_string("service_id", mirror_service.service_id);
        formatter->dump_string("instance_id", mirror_service.instance_id);
        formatter->dump_string("client_id", mirror_service.client_id);
        formatter->dump_string("hostname", mirror_service.hostname);
        formatter->dump_string("ceph_version", mirror_service.ceph_version);
        formatter->dump_bool("leader", mirror_service.leader);
        formatter->dump_stream("health") << mirror_service.health;
        if (!mirror_service.callouts.empty()) {
          formatter->open_array_section("callouts");
          for (auto& callout : mirror_service.callouts) {
            formatter->dump_string("callout", callout);
          }
          formatter->close_section(); // callouts
        }
        formatter->close_section(); // daemon
      }
      formatter->close_section(); // daemons
    } else {
      std::cout << std::endl << "DAEMONS" << std::endl;
      if (mirror_services.empty()) {
        std::cout << "  none" << std::endl;
      }
      for (auto& mirror_service : mirror_services) {
        std::cout << "service " << mirror_service.service_id << ":"
                  << std::endl
                  << "  instance_id: " << mirror_service.instance_id
                  << std::endl
                  << "  client_id: " << mirror_service.client_id << std::endl
                  << "  hostname: " << mirror_service.hostname << std::endl
                  << "  version: " << mirror_service.ceph_version << std::endl
                  << "  leader: " << (mirror_service.leader ? "true" : "false")
                  << std::endl
                  << "  health: " << mirror_service.health << std::endl;
        if (!mirror_service.callouts.empty()) {
          std::cout << "  callouts: " << mirror_service.callouts << std::endl;
        }
        std::cout << std::endl;
      }
      std::cout << std::endl;
    }

    // dump per-image status
    librados::IoCtx default_ns_io_ctx;
    default_ns_io_ctx.dup(io_ctx);
    default_ns_io_ctx.set_namespace("");
    std::vector<librbd::mirror_peer_site_t> mirror_peers;
    utils::get_mirror_peer_sites(default_ns_io_ctx, &mirror_peers);

    std::map<std::string, std::string> peer_mirror_uuids_to_name;
    utils::get_mirror_peer_mirror_uuids_to_names(mirror_peers,
                                                 &peer_mirror_uuids_to_name);

    if (formatter != nullptr) {
      formatter->open_array_section("images");
    } else {
      std::cout << "IMAGES";
    }

    std::map<std::string, std::string> instance_ids;

    std::string start_image_id;
    while (true) {
      std::map<std::string, std::string> ids;
      r = rbd.mirror_image_instance_id_list(io_ctx, start_image_id, 1024, &ids);
      if (r < 0) {
        if (r == -EOPNOTSUPP) {
          std::cerr << "rbd: newer release of Ceph OSDs required to map image "
                    << "to rbd-mirror daemon instance" << std::endl;
        } else {
          std::cerr << "rbd: failed to get instance id list: "
                    << cpp_strerror(r) << std::endl;
        }
        // not fatal
        break;
      }
      if (ids.empty()) {
        break;
      }
      instance_ids.insert(ids.begin(), ids.end());
      start_image_id = ids.rbegin()->first;
    }

    ImageRequestGenerator<StatusImageRequest> generator(
      io_ctx, instance_ids, mirror_peers, peer_mirror_uuids_to_name,
      daemon_service_info, formatter);
    ret = generator.execute();

    if (formatter != nullptr) {
      formatter->close_section(); // images
    }
  }

  if (formatter != nullptr) {
    formatter->close_section(); // status
    formatter->flush(std::cout);
  }

  return ret;
}

void get_promote_arguments(po::options_description *positional,
			   po::options_description *options) {
  options->add_options()
    ("force", po::bool_switch(),
     "promote even if not cleanly demoted by remote cluster");
  at::add_pool_options(positional, options, true);
}

int execute_promote(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  utils::disable_cache();

  std::atomic<unsigned> counter = { 0 };
  ImageRequestGenerator<PromoteImageRequest> generator(io_ctx, &counter,
                                                       vm["force"].as<bool>());
  r = generator.execute();

  std::cout << "Promoted " << counter.load() << " mirrored images" << std::endl;
  return r;
}

void get_demote_arguments(po::options_description *positional,
			   po::options_description *options) {
  at::add_pool_options(positional, options, true);
}

int execute_demote(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  utils::disable_cache();

  std::atomic<unsigned> counter { 0 };
  ImageRequestGenerator<DemoteImageRequest> generator(io_ctx, &counter);
  r = generator.execute();

  std::cout << "Demoted " << counter.load() << " mirrored images" << std::endl;
  return r;
}

Shell::Action action_bootstrap_create(
  {"mirror", "pool", "peer", "bootstrap", "create"}, {},
  "Create a peer bootstrap token to import in a remote cluster", "",
  &get_peer_bootstrap_create_arguments, &execute_peer_bootstrap_create);
Shell::Action action_bootstrap_import(
  {"mirror", "pool", "peer", "bootstrap", "import"}, {},
  "Import a peer bootstrap token created from a remote cluster", "",
  &get_peer_bootstrap_import_arguments, &execute_peer_bootstrap_import);

Shell::Action action_add(
  {"mirror", "pool", "peer", "add"}, {},
  "Add a mirroring peer to a pool.", "",
  &get_peer_add_arguments, &execute_peer_add);
Shell::Action action_remove(
  {"mirror", "pool", "peer", "remove"}, {},
  "Remove a mirroring peer from a pool.", "",
  &get_peer_remove_arguments, &execute_peer_remove);
Shell::Action action_set(
  {"mirror", "pool", "peer", "set"}, {},
  "Update mirroring peer settings.", "",
  &get_peer_set_arguments, &execute_peer_set);

Shell::Action action_disable(
  {"mirror", "pool", "disable"}, {},
  "Disable RBD mirroring by default within a pool.", "",
  &get_disable_arguments, &execute_disable);
Shell::Action action_enable(
  {"mirror", "pool", "enable"}, {},
  "Enable RBD mirroring by default within a pool.", "",
  &get_enable_arguments, &execute_enable);
Shell::Action action_info(
  {"mirror", "pool", "info"}, {},
  "Show information about the pool mirroring configuration.", {},
  &get_info_arguments, &execute_info);
Shell::Action action_status(
  {"mirror", "pool", "status"}, {},
  "Show status for all mirrored images in the pool.", {},
  &get_status_arguments, &execute_status);
Shell::Action action_promote(
  {"mirror", "pool", "promote"}, {},
  "Promote all non-primary images in the pool.", {},
  &get_promote_arguments, &execute_promote);
Shell::Action action_demote(
  {"mirror", "pool", "demote"}, {},
  "Demote all primary images in the pool.", {},
  &get_demote_arguments, &execute_demote);

} // namespace mirror_pool
} // namespace action
} // namespace rbd
