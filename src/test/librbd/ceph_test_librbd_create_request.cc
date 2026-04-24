// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Cond.h"
#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "librbd/AsioEngine.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/image/Types.h"
#include "librbd/internal.h"

#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

namespace {

enum class InjectKind {
  NONE,
  ERROR,
  ABORT,
  DELAY,
};

struct Arguments {
  std::string pool_name;
  std::string image_name;
  std::string image_id;
  uint64_t size = 0;
  bool pool_init = false;
  bool skip_mirror_enable = false;
  InjectKind inject_kind = InjectKind::NONE;
  std::string inject_stage;
  int inject_error = 0;
  uint64_t inject_delay_ms = 0;
};

void usage(std::ostream& out) {
  out << "usage: ceph_test_librbd_create_request <pool>/<image> --size <size> "
         "[options]\n"
      << "\n"
      << "options:\n"
      << "  --size <size>                image size (for example 1G)\n"
      << "  --image-id <id>             override the generated image id\n"
      << "  --pool-init                 run rbd pool init on the target pool\n"
      << "  --skip-mirror-enable        pass CREATE_FLAG_SKIP_MIRROR_ENABLE\n"
      << "  --inject-error-at <stage>   inject InjectError at a stage\n"
      << "  --inject-error-code <errno> error code for InjectError (5 or -5)\n"
      << "  --inject-abort-at <stage>   inject InjectAbort at a stage\n"
      << "  --inject-delay-at <stage>   inject InjectDelay at a stage\n"
      << "  --inject-delay-ms <ms>      delay duration in milliseconds\n"
      << "  --help                      show this help text\n"
      << "\n"
      << "valid stages: after_header, after_id_create, after_dir_add\n";
}

const char* get_stage_literal(std::string_view stage) {
  if (stage == "after_header") {
    return "after_header";
  }
  if (stage == "after_id_create") {
    return "after_id_create";
  }
  if (stage == "after_dir_add") {
    return "after_dir_add";
  }
  return nullptr;
}

bool parse_image_spec(const std::string& spec, Arguments* args,
                      std::string* error) {
  auto slash = spec.find('/');
  if (slash == std::string::npos || slash == 0 || slash == spec.size() - 1) {
    *error = "image spec must be <pool>/<image>";
    return false;
  }

  args->pool_name = spec.substr(0, slash);
  args->image_name = spec.substr(slash + 1);
  return true;
}

bool parse_size(const std::string& value, uint64_t* size, std::string* error) {
  std::string parse_error;
  *size = strict_iecstrtoll(value, &parse_error);
  if (!parse_error.empty()) {
    *error = "invalid --size value '" + value + "': " + parse_error;
    return false;
  }
  return true;
}

bool parse_u64(const std::string& value, uint64_t* result,
               std::string* error) {
  char* end = nullptr;
  errno = 0;
  auto parsed = std::strtoull(value.c_str(), &end, 10);
  if (errno != 0 || end == value.c_str() || *end != '\0') {
    *error = "invalid integer value '" + value + "'";
    return false;
  }

  *result = parsed;
  return true;
}

bool parse_i32(const std::string& value, int* result, std::string* error) {
  char* end = nullptr;
  errno = 0;
  auto parsed = std::strtoll(value.c_str(), &end, 10);
  if (errno != 0 || end == value.c_str() || *end != '\0') {
    *error = "invalid integer value '" + value + "'";
    return false;
  }

  if (parsed < std::numeric_limits<int>::min() ||
      parsed > std::numeric_limits<int>::max()) {
    *error = "integer value out of range: '" + value + "'";
    return false;
  }

  *result = static_cast<int>(parsed);
  return true;
}

bool set_inject_kind(Arguments* args, InjectKind inject_kind,
                     std::string* error) {
  if (args->inject_kind != InjectKind::NONE &&
      args->inject_kind != inject_kind) {
    *error = "only one fault injection mode can be specified";
    return false;
  }

  args->inject_kind = inject_kind;
  return true;
}

bool parse_arguments(int argc, char** argv, Arguments* args,
                     std::string* error) {
  std::optional<std::string> image_spec;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    auto require_value = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        *error = std::string("missing value for ") + name;
        return nullptr;
      }
      return argv[++i];
    };

    if (arg == "--help") {
      usage(std::cout);
      return false;
    } else if (arg == "--size") {
      auto value = require_value("--size");
      if (value == nullptr || !parse_size(value, &args->size, error)) {
        return false;
      }
    } else if (arg == "--image-id") {
      auto value = require_value("--image-id");
      if (value == nullptr) {
        return false;
      }
      args->image_id = value;
    } else if (arg == "--pool-init") {
      args->pool_init = true;
    } else if (arg == "--skip-mirror-enable") {
      args->skip_mirror_enable = true;
    } else if (arg == "--inject-error-at") {
      auto value = require_value("--inject-error-at");
      if (value == nullptr || !set_inject_kind(args, InjectKind::ERROR, error)) {
        return false;
      }
      args->inject_stage = value;
    } else if (arg == "--inject-error-code") {
      auto value = require_value("--inject-error-code");
      if (value == nullptr ||
          !set_inject_kind(args, InjectKind::ERROR, error) ||
          !parse_i32(value, &args->inject_error, error)) {
        return false;
      }
    } else if (arg == "--inject-abort-at") {
      auto value = require_value("--inject-abort-at");
      if (value == nullptr || !set_inject_kind(args, InjectKind::ABORT, error)) {
        return false;
      }
      args->inject_stage = value;
    } else if (arg == "--inject-delay-at") {
      auto value = require_value("--inject-delay-at");
      if (value == nullptr || !set_inject_kind(args, InjectKind::DELAY, error)) {
        return false;
      }
      args->inject_stage = value;
    } else if (arg == "--inject-delay-ms") {
      auto value = require_value("--inject-delay-ms");
      if (value == nullptr ||
          !set_inject_kind(args, InjectKind::DELAY, error) ||
          !parse_u64(value, &args->inject_delay_ms, error)) {
        return false;
      }
    } else if (!arg.empty() && arg[0] == '-') {
      *error = "unknown option '" + arg + "'";
      return false;
    } else if (!image_spec) {
      image_spec = arg;
    } else {
      *error = "unexpected positional argument '" + arg + "'";
      return false;
    }
  }

  if (!image_spec) {
    *error = "missing required <pool>/<image> argument";
    return false;
  }
  if (!parse_image_spec(*image_spec, args, error)) {
    return false;
  }
  if (args->size == 0) {
    *error = "missing required --size argument";
    return false;
  }

  if (args->inject_kind != InjectKind::NONE &&
      get_stage_literal(args->inject_stage) == nullptr) {
    *error = "invalid fault injection stage '" + args->inject_stage + "'";
    return false;
  }

  if (args->inject_kind == InjectKind::ERROR) {
    if (args->inject_stage.empty()) {
      *error = "--inject-error-at is required when using InjectError";
      return false;
    }
    if (args->inject_error == 0) {
      *error = "--inject-error-code must be non-zero";
      return false;
    }
    if (args->inject_error > 0) {
      args->inject_error = -args->inject_error;
    }
  } else if (args->inject_kind == InjectKind::DELAY) {
    if (args->inject_stage.empty()) {
      *error = "--inject-delay-at is required when using InjectDelay";
      return false;
    }
    if (args->inject_delay_ms == 0) {
      *error = "--inject-delay-ms must be greater than zero";
      return false;
    }
  } else if (args->inject_kind == InjectKind::ABORT &&
             args->inject_stage.empty()) {
    *error = "--inject-abort-at is required when using InjectAbort";
    return false;
  }

  return true;
}

int connect_cluster(librados::Rados* cluster) {
  const char* client_id = std::getenv("CEPH_CLIENT_ID");
  int r = cluster->init(client_id);
  if (r < 0) {
    std::cerr << "cluster.init failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  r = cluster->conf_read_file(nullptr);
  if (r < 0) {
    cluster->shutdown();
    std::cerr << "cluster.conf_read_file failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  cluster->conf_parse_env(nullptr);

  r = cluster->connect();
  if (r < 0) {
    cluster->shutdown();
    std::cerr << "cluster.connect failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int run_create_request(const Arguments& args) {
  librados::Rados cluster;
  int r = connect_cluster(&cluster);
  if (r < 0) {
    return 1;
  }

  auto exit_code = [&args, &cluster]() {
    librados::IoCtx ioctx;
    int r = cluster.ioctx_create(args.pool_name.c_str(), ioctx);
    if (r < 0) {
      std::cerr << "ioctx_create failed for pool '" << args.pool_name
                << "': " << cpp_strerror(r) << std::endl;
      return 1;
    }

    if (args.pool_init) {
      librbd::RBD rbd;
      r = rbd.pool_init(ioctx, true);
      if (r < 0) {
        std::cerr << "rbd pool init failed for pool '" << args.pool_name
                  << "': " << cpp_strerror(r) << std::endl;
        return 1;
      }
    }

    r = librbd::detect_format(ioctx, args.image_name, nullptr, nullptr);
    if (r != -ENOENT) {
      if (r == 0) {
        std::cerr << "rbd image '" << args.pool_name << "/"
                  << args.image_name << "' already exists" << std::endl;
      } else {
        std::cerr << "failed to determine whether image '" << args.pool_name
                  << "/" << args.image_name << "' exists: "
                  << cpp_strerror(r) << std::endl;
      }
      return 1;
    }

    std::string image_id = args.image_id;
    if (image_id.empty()) {
      image_id = librbd::util::generate_image_id(ioctx);
    }

    auto cct = reinterpret_cast<CephContext*>(ioctx.cct());
    auto config = cct->_conf;
    librbd::api::Config<>::apply_pool_overrides(ioctx, &config);

    librbd::ImageOptions opts;
    r = opts.set(RBD_IMAGE_OPTION_FORMAT, 2);
    if (r < 0) {
      std::cerr << "failed to set image format option: "
                << cpp_strerror(r) << std::endl;
      return 1;
    }

    uint32_t create_flags = args.skip_mirror_enable ?
      librbd::image::CREATE_FLAG_SKIP_MIRROR_ENABLE : 0U;

    librbd::image::CreateRequestFaultInjector fault_injector;
    if (args.inject_kind != InjectKind::NONE) {
      const char* stage = get_stage_literal(args.inject_stage);
      if (args.inject_kind == InjectKind::ERROR) {
        fault_injector.inject(stage, InjectError{args.inject_error});
      } else if (args.inject_kind == InjectKind::ABORT) {
        fault_injector.inject(stage, InjectAbort{});
      } else if (args.inject_kind == InjectKind::DELAY) {
        fault_injector.inject(stage, InjectDelay{
          std::chrono::milliseconds(args.inject_delay_ms)});
      }
    }

    librbd::AsioEngine asio_engine(ioctx);
    C_SaferCond cond;
    auto req = librbd::image::CreateRequest<>::create(
      config, ioctx, args.image_name, image_id, args.size, opts, create_flags,
      cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
      asio_engine.get_work_queue(), &cond, fault_injector);
    req->send();

    r = cond.wait();
    if (r < 0) {
      std::cerr << "create failed for '" << args.pool_name << "/"
                << args.image_name << "': " << cpp_strerror(r)
                << " (" << r << ")" << std::endl;
    } else {
      std::cout << "created " << args.pool_name << "/" << args.image_name
                << " id=" << image_id << std::endl;
      return 0;
    }
    return 1;
  }();

  cluster.shutdown();
  return exit_code;
}

} // anonymous namespace

int main(int argc, char** argv) {
  Arguments args;
  std::string error;
  if (!parse_arguments(argc, argv, &args, &error)) {
    if (!error.empty()) {
      std::cerr << error << std::endl;
      usage(std::cerr);
      return 1;
    }
    return 0;
  }

  return run_create_request(args);
}
