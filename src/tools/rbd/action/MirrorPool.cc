// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "include/rbd/librbd.hpp"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/Throttle.h"
#include "global/global_context.h"
#include <functional>
#include <iostream>
#include <regex>
#include <boost/program_options.hpp>
#include "include/assert.h"

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

namespace {

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
                            std::string *remote_cluster) {
  if (vm.count("remote-client-name")) {
    *remote_client_name = vm["remote-client-name"].as<std::string>();
  }
  if (vm.count("remote-cluster")) {
    *remote_cluster = vm["remote-cluster"].as<std::string>();
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

void format_mirror_peers(const std::string &config_path,
                         at::Format::Formatter formatter,
                         const std::vector<librbd::mirror_peer_t> &peers) {
  if (formatter != nullptr) {
    formatter->open_array_section("peers");
    for (auto &peer : peers) {
      formatter->open_object_section("peer");
      formatter->dump_string("uuid", peer.uuid);
      formatter->dump_string("cluster_name", peer.cluster_name);
      formatter->dump_string("client_name", peer.client_name);
      formatter->close_section();
    }
    formatter->close_section();
  } else {
    std::cout << "Peers: ";
    if (peers.empty()) {
      std::cout << "none" << std::endl;
    } else {
      TextTable tbl;
      tbl.define_column("", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("UUID", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("CLIENT", TextTable::LEFT, TextTable::LEFT);
      for (auto &peer : peers) {
        tbl << " "
            << peer.uuid
            << peer.cluster_name
            << peer.client_name
            << TextTable::endrow;
      }
      std::cout << std::endl << tbl;
    }
  }
}

class ImageRequestBase {
public:
  void send() {
    dout(20) << this << " " << __func__ << ": image_name=" << m_image_name
             << dendl;

    auto ctx = new FunctionContext([this](int r) {
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
  StatusImageRequest(librados::IoCtx &io_ctx, OrderedThrottle &throttle,
                     const std::string &image_name,
                     at::Format::Formatter formatter)
    : ImageRequestBase(io_ctx, throttle, image_name),
      m_formatter(formatter) {
  }

protected:
  bool skip_get_info() const override {
    return true;
  }

  void execute_action(librbd::Image &image,
                      librbd::RBD::AioCompletion *aio_comp) override {
    image.aio_mirror_image_get_status(&m_mirror_image_status,
                                      sizeof(m_mirror_image_status), aio_comp);
  }

  void finalize_action() override {
    if (m_mirror_image_status.info.global_id.empty()) {
      return;
    }

    std::string state = utils::mirror_image_status_state(m_mirror_image_status);
    std::string last_update = (
      m_mirror_image_status.last_update == 0 ?
        "" : utils::timestr(m_mirror_image_status.last_update));

    if (m_formatter != nullptr) {
      m_formatter->open_object_section("image");
      m_formatter->dump_string("name", m_mirror_image_status.name);
      m_formatter->dump_string("global_id",
                               m_mirror_image_status.info.global_id);
      m_formatter->dump_string("state", state);
      m_formatter->dump_string("description",
                               m_mirror_image_status.description);
      m_formatter->dump_string("last_update", last_update);
      m_formatter->close_section(); // image
    } else {
      std::cout << "\n" << m_mirror_image_status.name << ":\n"
	        << "  global_id:   "
                << m_mirror_image_status.info.global_id << "\n"
	        << "  state:       " << state << "\n"
	        << "  description: "
                << m_mirror_image_status.description << "\n"
	        << "  last_update: " << last_update << std::endl;
    }
  }

  std::string get_action_type() const override {
    return "status";
  }

private:
  at::Format::Formatter m_formatter;
  librbd::mirror_image_status_t m_mirror_image_status;

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
      m_throttle(g_conf->get_val<int64_t>("rbd_concurrent_management_ops"),
                 true) {
  }

  int execute() {
    // use the alphabetical list of image names for pool-level
    // mirror image operations
    librbd::RBD rbd;
    int r = rbd.list(m_io_ctx, m_image_names);
    if (r < 0 && r != -ENOENT) {
      std::cerr << "rbd: failed to list images within pool" << std::endl;
      return r;
    }

    for (auto &image_name : m_image_names) {
      auto request = m_factory(image_name);
      request->send();
    }

    return m_throttle.wait_for_ret();
  }
private:
  typedef std::function<RequestT*(const std::string&)>  Factory;

  librados::IoCtx &m_io_ctx;
  Factory m_factory;

  OrderedThrottle m_throttle;

  std::vector<std::string> m_image_names;

};

} // anonymous namespace

void get_peer_add_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_pool_options(positional, options);
  positional->add_options()
    ("remote-cluster-spec", "remote cluster spec\n"
     "(example: [<client name>@]<cluster name>)");
  options->add_options()
    ("remote-client-name", po::value<std::string>(), "remote client name")
    ("remote-cluster", po::value<std::string>(), "remote cluster name");
}

int execute_peer_add(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string remote_client_name = g_ceph_context->_conf->name.to_str();
  std::string remote_cluster;
  int r = get_remote_cluster_spec(
    vm, utils::get_positional_argument(vm, arg_index),
    &remote_client_name, &remote_cluster);
  if (r < 0) {
    return r;
  }

  std::string config_path;
  if (vm.count(at::CONFIG_PATH)) {
    config_path = vm[at::CONFIG_PATH].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
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
  std::vector<librbd::mirror_peer_t> mirror_peers;
  r = rbd.mirror_peer_list(io_ctx, &mirror_peers);
  if (r < 0) {
    std::cerr << "rbd: failed to list mirror peers" << std::endl;
    return r;
  }
  if (!mirror_peers.empty()) {
    std::cerr << "rbd: multiple peers are not currently supported" << std::endl;
    return -EINVAL;
  }

  std::string uuid;
  r = rbd.mirror_peer_add(io_ctx, &uuid, remote_cluster, remote_client_name);
  if (r < 0) {
    std::cerr << "rbd: error adding mirror peer" << std::endl;
    return r;
  }

  std::cout << uuid << std::endl;
  return 0;
}

void get_peer_remove_arguments(po::options_description *positional,
                               po::options_description *options) {
  at::add_pool_options(positional, options);
  add_uuid_option(positional);
}

int execute_peer_remove(const po::variables_map &vm,
                        const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string uuid;
  int r = get_uuid(vm, arg_index, &uuid);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.mirror_peer_remove(io_ctx, uuid);
  if (r < 0) {
    std::cerr << "rbd: error removing mirror peer" << std::endl;
    return r;
  }
  return 0;
}

void get_peer_set_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_pool_options(positional, options);
  add_uuid_option(positional);
  positional->add_options()
    ("key", "peer parameter [client or cluster]")
    ("value", "new client or cluster name");
}

int execute_peer_set(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string uuid;
  int r = get_uuid(vm, arg_index++, &uuid);
  if (r < 0) {
    return r;
  }

  std::string key = utils::get_positional_argument(vm, arg_index++);
  if (key != "client" && key != "cluster") {
    std::cerr << "rbd: must specify 'client' or 'cluster' key." << std::endl;
    return -EINVAL;
  }

  std::string value = utils::get_positional_argument(vm, arg_index++);
  if (value.empty()) {
    std::cerr << "rbd: must specify new " << key << " value." << std::endl;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  if (key == "client") {
    r = rbd.mirror_peer_set_client(io_ctx, uuid.c_str(), value.c_str());
  } else {
    r = rbd.mirror_peer_set_cluster(io_ctx, uuid.c_str(), value.c_str());
  }
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_disable_arguments(po::options_description *positional,
                           po::options_description *options) {
  at::add_pool_options(positional, options);
}

void get_enable_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_pool_options(positional, options);
  positional->add_options()
    ("mode", "mirror mode [image or pool]");
}

int execute_enable_disable(const std::string &pool_name,
                           rbd_mirror_mode_t next_mirror_mode,
                           const std::string &mode) {
  librados::Rados rados;
  librados::IoCtx io_ctx;
  rbd_mirror_mode_t current_mirror_mode;

  int r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.mirror_mode_get(io_ctx, &current_mirror_mode);
  if (r < 0) {
    std::cerr << "rbd: failed to retrieve mirror mode: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  if (current_mirror_mode == next_mirror_mode) {
    if (mode == "disabled") {
      std::cout << "mirroring is already " << mode << std::endl;
    } else {
      std::cout << "mirroring is already configured for "
                << mode << " mode" << std::endl;
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
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  return execute_enable_disable(pool_name, RBD_MIRROR_MODE_DISABLED,
                                "disabled");
}

int execute_enable(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

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

  return execute_enable_disable(pool_name, mirror_mode, mode);
}

void get_info_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
}

int execute_info(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  std::string config_path;
  if (vm.count(at::CONFIG_PATH)) {
    config_path = vm[at::CONFIG_PATH].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  rbd_mirror_mode_t mirror_mode;
  r = rbd.mirror_mode_get(io_ctx, &mirror_mode);
  if (r < 0) {
    return r;
  }

  std::vector<librbd::mirror_peer_t> mirror_peers;
  r = rbd.mirror_peer_list(io_ctx, &mirror_peers);
  if (r < 0) {
    return r;
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

  if (mirror_mode != RBD_MIRROR_MODE_DISABLED) {
    format_mirror_peers(config_path, formatter, mirror_peers);
  }
  if (formatter != nullptr) {
    formatter->close_section();
    formatter->flush(std::cout);
  }
  return 0;
}

void get_status_arguments(po::options_description *positional,
			  po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
  at::add_verbose_option(options);
}

int execute_status(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  bool verbose = vm[at::VERBOSE].as<bool>();

  std::string config_path;
  if (vm.count(at::CONFIG_PATH)) {
    config_path = vm[at::CONFIG_PATH].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;

  std::map<librbd::mirror_image_status_state_t, int> states;
  r = rbd.mirror_image_status_summary(io_ctx, &states);
  if (r < 0) {
    std::cerr << "rbd: failed to get status summary for mirrored images: "
	      << cpp_strerror(r) << std::endl;
    return r;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("status");
  }

  enum Health {Ok = 0, Warning = 1, Error = 2} health = Ok;
  const char *names[] = {"OK", "WARNING", "ERROR"};
  int total = 0;

  for (auto &it : states) {
    auto &state = it.first;
    if (health < Warning &&
	(state != MIRROR_IMAGE_STATUS_STATE_REPLAYING &&
	 state != MIRROR_IMAGE_STATUS_STATE_STOPPED)) {
      health = Warning;
    }
    if (health < Error &&
	state == MIRROR_IMAGE_STATUS_STATE_ERROR) {
      health = Error;
    }
    total += it.second;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("summary");
    formatter->dump_string("health", names[health]);
    formatter->open_object_section("states");
    for (auto &it : states) {
      std::string state_name = utils::mirror_image_status_state(it.first);
      formatter->dump_int(state_name.c_str(), it.second);
    }
    formatter->close_section(); // states
    formatter->close_section(); // summary
  } else {
    std::cout << "health: " << names[health] << std::endl;
    std::cout << "images: " << total << " total" << std::endl;
    for (auto &it : states) {
      std::cout << "    " << it.second << " "
		<< utils::mirror_image_status_state(it.first) << std::endl;
    }
  }

  int ret = 0;

  if (verbose) {
    if (formatter != nullptr) {
      formatter->open_array_section("images");
    }

    ImageRequestGenerator<StatusImageRequest> generator(io_ctx, formatter);
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
  at::add_pool_options(positional, options);
}

int execute_promote(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  std::atomic<unsigned> counter = { 0 };
  ImageRequestGenerator<PromoteImageRequest> generator(io_ctx, &counter,
                                                       vm["force"].as<bool>());
  r = generator.execute();

  std::cout << "Promoted " << counter.load() << " mirrored images" << std::endl;
  return r;
}

void get_demote_arguments(po::options_description *positional,
			   po::options_description *options) {
  at::add_pool_options(positional, options);
}

int execute_demote(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx);
  if (r < 0) {
    return r;
  }

  std::atomic<unsigned> counter { 0 };
  ImageRequestGenerator<DemoteImageRequest> generator(io_ctx, &counter);
  r = generator.execute();

  std::cout << "Demoted " << counter.load() << " mirrored images" << std::endl;
  return r;
}

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
