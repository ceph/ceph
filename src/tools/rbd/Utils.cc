// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/Utils.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "include/encoding.h"
#include "common/common_init.h"
#include "include/stringify.h"
#include "include/rbd/features.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/escape.h"
#include "common/safe_io.h"
#include "global/global_context.h"
#include <fstream>
#include <iostream>
#include <regex>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace rbd {
namespace utils {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

static std::string mgr_command_args_to_str(
    const std::map<std::string, std::string> &args) {
  std::string out = "";

  std::string delimiter;
  for (auto &it : args) {
    out += delimiter + "\"" + it.first + "\": \"" +
      stringify(json_stream_escaper(it.second)) + "\"";
    delimiter = ",\n";
  }

  return out;
}

} // anonymous namespace

int ProgressContext::update_progress(uint64_t offset, uint64_t total) {
  if (progress) {
    int pc = get_percentage(offset, total);
    if (pc > last_pc) {
      std::cerr << "\r" << operation << ": "
		<< pc << "% complete..." << std::flush;
      last_pc = pc;
    }
  }
  return 0;
}

void ProgressContext::finish() {
  if (progress) {
    std::cerr << "\r" << operation << ": 100% complete...done." << std::endl;
  }
}

void ProgressContext::fail() {
  if (progress) {
    std::cerr << "\r" << operation << ": " << last_pc << "% complete...failed."
	      << std::endl;
  }
}

int get_percentage(uint64_t part, uint64_t whole) {
  return whole ? (100 * part / whole) : 0;
}

void aio_context_callback(librbd::completion_t completion, void *arg)
{
  librbd::RBD::AioCompletion *aio_completion =
    reinterpret_cast<librbd::RBD::AioCompletion*>(completion);
  Context *context = reinterpret_cast<Context *>(arg);
  context->complete(aio_completion->get_return_value());
  aio_completion->release();
}

int read_string(int fd, unsigned max, std::string *out) {
  char buf[4];

  int r = safe_read_exact(fd, buf, 4);
  if (r < 0)
    return r;

  bufferlist bl;
  bl.append(buf, 4);
  auto p = bl.cbegin();
  uint32_t len;
  decode(len, p);
  if (len > max)
    return -EINVAL;

  char sbuf[len];
  r = safe_read_exact(fd, sbuf, len);
  if (r < 0)
    return r;
  out->assign(sbuf, len);
  return len;
}

int extract_spec(const std::string &spec, std::string *pool_name,
                 std::string *namespace_name, std::string *name,
                 std::string *snap_name, SpecValidation spec_validation) {
  if (!g_ceph_context->_conf.get_val<bool>("rbd_validate_names")) {
    spec_validation = SPEC_VALIDATION_NONE;
  }

  std::regex pattern;
  switch (spec_validation) {
  case SPEC_VALIDATION_FULL:
    // disallow "/" and "@" in all names
    pattern = "^(?:([^/@]+)/(?:([^/@]+)/)?)?([^/@]+)(?:@([^/@]+))?$";
    break;
  case SPEC_VALIDATION_SNAP:
    // disallow "/" and "@" in snap name
    pattern = "^(?:([^/]+)/(?:([^/@]+)/)?)?([^@]+)(?:@([^/@]+))?$";
    break;
  case SPEC_VALIDATION_NONE:
    // relaxed pattern assumes pool is before first "/",
    // namespace is before second "/", and snap name is after first "@"
    pattern = "^(?:([^/]+)/(?:([^/@]+)/)?)?([^@]+)(?:@(.+))?$";
    break;
  default:
    ceph_abort();
    break;
  }

  std::smatch match;
  if (!std::regex_match(spec, match, pattern)) {
    std::cerr << "rbd: invalid spec '" << spec << "'" << std::endl;
    return -EINVAL;
  }

  if (match[1].matched) {
    if (pool_name != nullptr) {
      *pool_name = match[1];
    } else {
      std::cerr << "rbd: pool name specified for a command that doesn't use it"
                << std::endl;
      return -EINVAL;
    }
  }

  if (match[2].matched) {
    if (namespace_name != nullptr) {
      *namespace_name = match[2];
    } else {
      std::cerr << "rbd: namespace name specified for a command that doesn't "
                << "use it" << std::endl;
      return -EINVAL;
    }
  }

  if (name != nullptr) {
    *name = match[3];
  }

  if (match[4].matched) {
    if (snap_name != nullptr) {
      *snap_name = match[4];
    } else {
      std::cerr << "rbd: snapshot name specified for a command that doesn't "
                << "use it" << std::endl;
      return -EINVAL;
    }
  }
  return 0;
}

std::string get_positional_argument(const po::variables_map &vm, size_t index) {
  if (vm.count(at::POSITIONAL_ARGUMENTS) == 0) {
    return "";
  }

  const std::vector<std::string> &args =
    boost::any_cast<std::vector<std::string> >(
      vm[at::POSITIONAL_ARGUMENTS].value());
  if (index < args.size()) {
    return args[index];
  }
  return "";
}

void normalize_pool_name(std::string* pool_name) {
  if (pool_name->empty()) {
    *pool_name = get_default_pool_name();
  }
}

std::string get_default_pool_name() {
  return g_ceph_context->_conf.get_val<std::string>("rbd_default_pool");
}

int get_pool_and_namespace_names(
    const boost::program_options::variables_map &vm, bool validate_pool_name,
    std::string* pool_name, std::string* namespace_name, size_t *arg_index) {
  if (namespace_name != nullptr && vm.count(at::NAMESPACE_NAME)) {
    *namespace_name = vm[at::NAMESPACE_NAME].as<std::string>();
  }

  if (vm.count(at::POOL_NAME)) {
    *pool_name = vm[at::POOL_NAME].as<std::string>();
  } else {
    *pool_name = get_positional_argument(vm, *arg_index);
    if (!pool_name->empty()) {
      if (namespace_name != nullptr) {
        auto slash_pos = pool_name->find_last_of('/');
        if (slash_pos != std::string::npos) {
          *namespace_name = pool_name->substr(slash_pos + 1);
        }
        *pool_name = pool_name->substr(0, slash_pos);
      }
      ++(*arg_index);
    }
  }

  if (!g_ceph_context->_conf.get_val<bool>("rbd_validate_names")) {
    validate_pool_name = false;
  }

  if (validate_pool_name &&
      pool_name->find_first_of("/@") != std::string::npos) {
    std::cerr << "rbd: invalid pool '" << *pool_name << "'" << std::endl;
    return -EINVAL;
  } else if (namespace_name != nullptr &&
             namespace_name->find_first_of("/@") != std::string::npos) {
    std::cerr << "rbd: invalid namespace '" << *namespace_name << "'"
              << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_pool_image_id(const po::variables_map &vm,
                      size_t *spec_arg_index,
                      std::string *pool_name,
                      std::string *namespace_name,
                      std::string *image_id) {

  if (vm.count(at::POOL_NAME) && pool_name != nullptr) {
    *pool_name = vm[at::POOL_NAME].as<std::string>();
  }
  if (vm.count(at::NAMESPACE_NAME) && namespace_name != nullptr) {
    *namespace_name = vm[at::NAMESPACE_NAME].as<std::string>();
  }
  if (vm.count(at::IMAGE_ID) && image_id != nullptr) {
    *image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  int r;
  if (image_id != nullptr && spec_arg_index != nullptr && image_id->empty()) {
    std::string spec = get_positional_argument(vm, (*spec_arg_index)++);
    if (!spec.empty()) {
      r = extract_spec(spec, pool_name, namespace_name, image_id, nullptr,
                       SPEC_VALIDATION_FULL);
      if (r < 0) {
        return r;
      }
    }
  }

  if (image_id != nullptr && image_id->empty()) {
    std::cerr << "rbd: image id was not specified" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_image_or_snap_spec(const po::variables_map &vm, std::string *spec) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string nspace_name;
  std::string image_name;
  std::string snap_name;
  int r = get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &nspace_name,
    &image_name, &snap_name, true, SNAPSHOT_PRESENCE_PERMITTED,
    SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (pool_name.empty()) {
    // connect to the cluster to get the default pool
    librados::Rados rados;
    r = init_rados(&rados);
    if (r < 0) {
      return r;
    }

    normalize_pool_name(&pool_name);
  }

  spec->append(pool_name);
  spec->append("/");
  if (!nspace_name.empty()) {
    spec->append(nspace_name);
    spec->append("/");
  }
  spec->append(image_name);
  if (!snap_name.empty()) {
    spec->append("@");
    spec->append(snap_name);
  }

  return 0;
}

void append_options_as_args(const std::vector<std::string> &options,
                            std::vector<std::string> *args) {
  for (auto &opts : options) {
    std::vector<std::string> args_;
    boost::split(args_, opts, boost::is_any_of(","));
    for (auto &o : args_) {
      args->push_back("--" + o);
    }
  }
}

int get_pool_image_snapshot_names(const po::variables_map &vm,
                                  at::ArgumentModifier mod,
                                  size_t *spec_arg_index,
                                  std::string *pool_name,
                                  std::string *namespace_name,
                                  std::string *image_name,
                                  std::string *snap_name,
                                  bool image_name_required,
                                  SnapshotPresence snapshot_presence,
                                  SpecValidation spec_validation) {
  std::string pool_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_POOL_NAME : at::POOL_NAME);
  std::string image_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_IMAGE_NAME : at::IMAGE_NAME);
  return get_pool_generic_snapshot_names(vm, mod, spec_arg_index, pool_key,
                                         pool_name, namespace_name, image_key,
                                         "image", image_name, snap_name,
                                         image_name_required, snapshot_presence,
                                         spec_validation);
}

int get_pool_generic_snapshot_names(const po::variables_map &vm,
                                    at::ArgumentModifier mod,
                                    size_t *spec_arg_index,
                                    const std::string& pool_key,
                                    std::string *pool_name,
                                    std::string *namespace_name,
                                    const std::string& generic_key,
                                    const std::string& generic_key_desc,
                                    std::string *generic_name,
                                    std::string *snap_name,
                                    bool generic_name_required,
                                    SnapshotPresence snapshot_presence,
                                    SpecValidation spec_validation) {
  std::string namespace_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_NAMESPACE_NAME : at::NAMESPACE_NAME);
  std::string snap_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_SNAPSHOT_NAME : at::SNAPSHOT_NAME);

  if (vm.count(pool_key) && pool_name != nullptr) {
    *pool_name = vm[pool_key].as<std::string>();
  }
  if (vm.count(namespace_key) && namespace_name != nullptr) {
    *namespace_name = vm[namespace_key].as<std::string>();
  }
  if (vm.count(generic_key) && generic_name != nullptr) {
    *generic_name = vm[generic_key].as<std::string>();
  }
  if (vm.count(snap_key) && snap_name != nullptr) {
    *snap_name = vm[snap_key].as<std::string>();
  }

  int r;
  if ((generic_key == at::IMAGE_NAME || generic_key == at::DEST_IMAGE_NAME) &&
      generic_name != nullptr && !generic_name->empty()) {
    // despite the separate pool and snapshot name options,
    // we can also specify them via the image option
    std::string image_name_copy(*generic_name);
    r = extract_spec(image_name_copy, pool_name, namespace_name, generic_name,
                     snap_name, spec_validation);
    if (r < 0) {
      return r;
    }
  }

  if (generic_name != nullptr && spec_arg_index != nullptr &&
      generic_name->empty()) {
    std::string spec = get_positional_argument(vm, (*spec_arg_index)++);
    if (!spec.empty()) {
      r = extract_spec(spec, pool_name, namespace_name, generic_name, snap_name,
                       spec_validation);
      if (r < 0) {
        return r;
      }
    }
  }

  if (generic_name != nullptr && generic_name_required &&
      generic_name->empty()) {
    std::string prefix = at::get_description_prefix(mod);
    std::cerr << "rbd: "
              << (mod == at::ARGUMENT_MODIFIER_DEST ? prefix : std::string())
              << generic_key_desc << " name was not specified" << std::endl;
    return -EINVAL;
  }

  std::regex pattern("^[^@/]*?$");
  if (spec_validation == SPEC_VALIDATION_FULL) {
    // validate pool name while creating/renaming/copying/cloning/importing/etc
    if ((pool_name != nullptr) && !std::regex_match (*pool_name, pattern)) {
      std::cerr << "rbd: invalid pool name '" << *pool_name << "'" << std::endl;
      return -EINVAL;
    }
  }

  if (namespace_name != nullptr && !namespace_name->empty() &&
      !std::regex_match (*namespace_name, pattern)) {
    std::cerr << "rbd: invalid namespace name '" << *namespace_name << "'"
              << std::endl;
    return -EINVAL;
  }

  if (snap_name != nullptr) {
    r = validate_snapshot_name(mod, *snap_name, snapshot_presence,
	                       spec_validation);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

int validate_snapshot_name(at::ArgumentModifier mod,
                           const std::string &snap_name,
                           SnapshotPresence snapshot_presence,
			   SpecValidation spec_validation) {
  std::string prefix = at::get_description_prefix(mod);
  switch (snapshot_presence) {
  case SNAPSHOT_PRESENCE_PERMITTED:
    break;
  case SNAPSHOT_PRESENCE_NONE:
    if (!snap_name.empty()) {
      std::cerr << "rbd: "
                << (mod == at::ARGUMENT_MODIFIER_DEST ? prefix : std::string())
                << "snapshot name specified for a command that doesn't use it"
                << std::endl;
      return -EINVAL;
    }
    break;
  case SNAPSHOT_PRESENCE_REQUIRED:
    if (snap_name.empty()) {
      std::cerr << "rbd: "
                << (mod == at::ARGUMENT_MODIFIER_DEST ? prefix : std::string())
                << "snapshot name was not specified" << std::endl;
      return -EINVAL;
    }
    break;
  }

  if (spec_validation == SPEC_VALIDATION_SNAP) {
    // disallow "/" and "@" in snap name
    std::regex pattern("^[^@/]*?$");
    if (!std::regex_match (snap_name, pattern)) {
      std::cerr << "rbd: invalid snap name '" << snap_name << "'" << std::endl;
      return -EINVAL;
    }
  }
  return 0;
}

int get_image_options(const boost::program_options::variables_map &vm,
		      bool get_format, librbd::ImageOptions *opts) {
  uint64_t order = 0, stripe_unit = 0, stripe_count = 0, object_size = 0;
  uint64_t features = 0, features_set = 0, features_clear = 0;
  std::string data_pool;
  bool order_specified = true;
  bool features_specified = false;
  bool features_set_specified = false;
  bool features_clear_specified = false;
  bool stripe_specified = false;

  if (vm.count(at::IMAGE_ORDER)) {
    order = vm[at::IMAGE_ORDER].as<uint64_t>();
  } else if (vm.count(at::IMAGE_OBJECT_SIZE)) {
    object_size = vm[at::IMAGE_OBJECT_SIZE].as<uint64_t>();
    order = std::round(std::log2(object_size));
  } else {
    order_specified = false;
  }

  if (vm.count(at::IMAGE_FEATURES)) {
    features = vm[at::IMAGE_FEATURES].as<uint64_t>();
    features_specified = true;
  }

  if (vm.count(at::IMAGE_STRIPE_UNIT)) {
    stripe_unit = vm[at::IMAGE_STRIPE_UNIT].as<uint64_t>();
    stripe_specified = true;
  }

  if (vm.count(at::IMAGE_STRIPE_COUNT)) {
    stripe_count = vm[at::IMAGE_STRIPE_COUNT].as<uint64_t>();
    stripe_specified = true;
  }

  if (vm.count(at::IMAGE_MIRROR_IMAGE_MODE) &&
      vm[at::IMAGE_MIRROR_IMAGE_MODE].as<librbd::mirror_image_mode_t>() ==
      RBD_MIRROR_IMAGE_MODE_JOURNAL) {
    features_set |= (RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);
    features_set_specified = true;
  }

  if (vm.count(at::IMAGE_SHARED) && vm[at::IMAGE_SHARED].as<bool>()) {
    if (features_specified) {
      features &= ~RBD_FEATURES_SINGLE_CLIENT;
    } else {
      features_clear |= RBD_FEATURES_SINGLE_CLIENT;
      features_clear_specified = true;
    }
  }

  if (vm.count(at::IMAGE_DATA_POOL)) {
    data_pool = vm[at::IMAGE_DATA_POOL].as<std::string>();
  }

  if (get_format) {
    uint64_t format = 0;
    bool format_specified = false;
    if (vm.count(at::IMAGE_NEW_FORMAT)) {
      format = 2;
      format_specified = true;
    } else if (vm.count(at::IMAGE_FORMAT)) {
      format = vm[at::IMAGE_FORMAT].as<uint32_t>();
      format_specified = true;
    }
    if (format == 1) {
      std::cerr << "rbd: image format 1 is deprecated" << std::endl;
    }

    if (features_specified && features != 0) {
      if (format_specified && format == 1) {
        std::cerr << "rbd: features not allowed with format 1; "
                  << "use --image-format 2" << std::endl;
        return -EINVAL;
      } else {
        format = 2;
        format_specified = true;
      }
    }

    if ((stripe_unit || stripe_count) &&
        (stripe_unit != (1ull << order) && stripe_count != 1)) {
      if (format_specified && format == 1) {
        std::cerr << "rbd: non-default striping not allowed with format 1; "
                  << "use --image-format 2" << std::endl;
        return -EINVAL;
      } else {
        format = 2;
        format_specified = true;
      }
    }

    if (!data_pool.empty()) {
      if (format_specified && format == 1) {
        std::cerr << "rbd: data pool not allowed with format 1; "
                  << "use --image-format 2" << std::endl;
        return -EINVAL;
      } else {
        format = 2;
        format_specified = true;
      }
    }

    if (format_specified) {
      int r = g_conf().set_val("rbd_default_format", stringify(format));
      ceph_assert(r == 0);
      opts->set(RBD_IMAGE_OPTION_FORMAT, format);
    }
  }

  if (order_specified)
    opts->set(RBD_IMAGE_OPTION_ORDER, order);
  if (features_specified)
    opts->set(RBD_IMAGE_OPTION_FEATURES, features);
  if (features_set_specified)
    opts->set(RBD_IMAGE_OPTION_FEATURES_SET, features_set);
  if (features_clear_specified) {
    opts->set(RBD_IMAGE_OPTION_FEATURES_CLEAR, features_clear);
  }
  if (stripe_specified) {
    opts->set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
    opts->set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
  }
  if (!data_pool.empty()) {
    opts->set(RBD_IMAGE_OPTION_DATA_POOL, data_pool);
  }
  int r = get_journal_options(vm, opts);
  if (r < 0) {
    return r;
  }

  r = get_flatten_option(vm, opts);
  if (r < 0) {
    return r;
  }

  if (vm.count(at::IMAGE_MIRROR_IMAGE_MODE)) {
    opts->set(RBD_IMAGE_OPTION_MIRROR_IMAGE_MODE,
              vm[at::IMAGE_MIRROR_IMAGE_MODE].as<librbd::mirror_image_mode_t>());
  }

  return 0;
}

int get_journal_options(const boost::program_options::variables_map &vm,
			librbd::ImageOptions *opts) {

  if (vm.count(at::JOURNAL_OBJECT_SIZE)) {
    uint64_t size = vm[at::JOURNAL_OBJECT_SIZE].as<uint64_t>();
    uint64_t order = 12;
    while ((1ULL << order) < size) {
      order++;
    }
    opts->set(RBD_IMAGE_OPTION_JOURNAL_ORDER, order);

    int r = g_conf().set_val("rbd_journal_order", stringify(order));
    ceph_assert(r == 0);
  }
  if (vm.count(at::JOURNAL_SPLAY_WIDTH)) {
    opts->set(RBD_IMAGE_OPTION_JOURNAL_SPLAY_WIDTH,
	      vm[at::JOURNAL_SPLAY_WIDTH].as<uint64_t>());

    int r = g_conf().set_val("rbd_journal_splay_width",
			    stringify(
			      vm[at::JOURNAL_SPLAY_WIDTH].as<uint64_t>()));
    ceph_assert(r == 0);
  }
  if (vm.count(at::JOURNAL_POOL)) {
    opts->set(RBD_IMAGE_OPTION_JOURNAL_POOL,
	      vm[at::JOURNAL_POOL].as<std::string>());

    int r = g_conf().set_val("rbd_journal_pool",
			    vm[at::JOURNAL_POOL].as<std::string>());
    ceph_assert(r == 0);
  }

  return 0;
}

int get_flatten_option(const boost::program_options::variables_map &vm,
                       librbd::ImageOptions *opts) {
  if (vm.count(at::IMAGE_FLATTEN) && vm[at::IMAGE_FLATTEN].as<bool>()) {
    uint64_t flatten = 1;
    opts->set(RBD_IMAGE_OPTION_FLATTEN, flatten);
  }
  return 0;
}

int get_image_size(const boost::program_options::variables_map &vm,
                   uint64_t *size) {
  if (vm.count(at::IMAGE_SIZE) == 0) {
    std::cerr << "rbd: must specify --size <M/G/T>" << std::endl;
    return -EINVAL;
  }

  *size = vm[at::IMAGE_SIZE].as<uint64_t>();
  return 0;
}

int get_path(const boost::program_options::variables_map &vm,
             size_t *arg_index, std::string *path) {
  if (vm.count(at::PATH)) {
    *path = vm[at::PATH].as<std::string>();
  } else {
    *path = get_positional_argument(vm, *arg_index);
    if (!path->empty()) {
      ++(*arg_index);
    }
  }

  if (path->empty()) {
    std::cerr << "rbd: path was not specified" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int get_formatter(const po::variables_map &vm,
                  at::Format::Formatter *formatter) {
  if (vm.count(at::FORMAT)) {
    bool pretty = vm[at::PRETTY_FORMAT].as<bool>();
    *formatter = vm[at::FORMAT].as<at::Format>().create_formatter(pretty);
    if (*formatter == nullptr && pretty) {
      std::cerr << "rbd: --pretty-format only works when --format "
                << "is json or xml" << std::endl;
      return -EINVAL;
    } else if (*formatter != nullptr && !pretty) {
      formatter->get()->enable_line_break();
    }
  } else if (vm[at::PRETTY_FORMAT].as<bool>()) {
    std::cerr << "rbd: --pretty-format only works when --format "
              << "is json or xml" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int get_snap_create_flags(const po::variables_map &vm, uint32_t *flags) {
  if (vm[at::SKIP_QUIESCE].as<bool>() &&
      vm[at::IGNORE_QUIESCE_ERROR].as<bool>()) {
    std::cerr << "rbd: " << at::IGNORE_QUIESCE_ERROR
              << " cannot be used together with " << at::SKIP_QUIESCE
              << std::endl;
    return -EINVAL;
  }

  *flags = 0;
  if (vm[at::SKIP_QUIESCE].as<bool>()) {
    *flags |= RBD_SNAP_CREATE_SKIP_QUIESCE;
  } else if (vm[at::IGNORE_QUIESCE_ERROR].as<bool>()) {
    *flags |= RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR;
  }
  return 0;
}

int get_encryption_options(const boost::program_options::variables_map &vm,
                           EncryptionOptions* result) {
  std::vector<std::string> passphrase_files;
  if (vm.count(at::ENCRYPTION_PASSPHRASE_FILE)) {
    passphrase_files =
            vm[at::ENCRYPTION_PASSPHRASE_FILE].as<std::vector<std::string>>();
  }

  std::vector<at::EncryptionFormat> formats;
  if (vm.count(at::ENCRYPTION_FORMAT)) {
    formats = vm[at::ENCRYPTION_FORMAT].as<decltype(formats)>();
  } else if (vm.count(at::ENCRYPTION_PASSPHRASE_FILE)) {
    formats.resize(passphrase_files.size(),
                   at::EncryptionFormat{RBD_ENCRYPTION_FORMAT_LUKS});
  }

  if (formats.size() != passphrase_files.size()) {
    std::cerr << "rbd: encryption formats count does not match "
              << "passphrase files count" << std::endl;
    return -EINVAL;
  }

  result->specs.clear();
  result->specs.reserve(formats.size());
  for (size_t i = 0; i < formats.size(); ++i) {
    std::ifstream file(passphrase_files[i], std::ios::in | std::ios::binary);
    if (file.fail()) {
      std::cerr << "rbd: unable to open passphrase file '"
                << passphrase_files[i] << "': " << cpp_strerror(errno)
                << std::endl;
      return -errno;
    }
    std::string passphrase((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
    file.close();

    switch (formats[i].format) {
    case RBD_ENCRYPTION_FORMAT_LUKS: {
      auto opts = new librbd::encryption_luks_format_options_t{
          std::move(passphrase)};
      result->specs.push_back(
          {RBD_ENCRYPTION_FORMAT_LUKS, opts, sizeof(*opts)});
      break;
    }
    case RBD_ENCRYPTION_FORMAT_LUKS1: {
      auto opts = new librbd::encryption_luks1_format_options_t{
          .passphrase = std::move(passphrase)};
      result->specs.push_back(
          {RBD_ENCRYPTION_FORMAT_LUKS1, opts, sizeof(*opts)});
      break;
    }
    case RBD_ENCRYPTION_FORMAT_LUKS2: {
      auto opts = new librbd::encryption_luks2_format_options_t{
          .passphrase = std::move(passphrase)};
      result->specs.push_back(
          {RBD_ENCRYPTION_FORMAT_LUKS2, opts, sizeof(*opts)});
      break;
    }
    default:
      ceph_abort();
    }
  }

  return 0;
}

void init_context() {
  g_conf().set_val_or_die("rbd_cache_writethrough_until_flush", "false");
  g_conf().apply_changes(nullptr);
}

int init_rados(librados::Rados *rados) {
  init_context();

  int r = rados->init_with_context(g_ceph_context);
  if (r < 0) {
    std::cerr << "rbd: couldn't initialize rados!" << std::endl;
    return r;
  }

  r = rados->connect();
  if (r < 0) {
    std::cerr << "rbd: couldn't connect to the cluster!" << std::endl;
    return r;
  }

  return 0;
}

int init(const std::string &pool_name, const std::string& namespace_name,
         librados::Rados *rados, librados::IoCtx *io_ctx) {
  init_context();

  int r = init_rados(rados);
  if (r < 0) {
    return r;
  }

  r = init_io_ctx(*rados, pool_name, namespace_name, io_ctx);
  if (r < 0) {
    return r;
  }
  return 0;
}

int init_io_ctx(librados::Rados &rados, std::string pool_name,
                const std::string& namespace_name, librados::IoCtx *io_ctx) {
  normalize_pool_name(&pool_name);

  int r = rados.ioctx_create(pool_name.c_str(), *io_ctx);
  if (r < 0) {
    if (r == -ENOENT && pool_name == get_default_pool_name()) {
      std::cerr << "rbd: error opening default pool "
                << "'" << pool_name << "'" << std::endl
                << "Ensure that the default pool has been created or specify "
                << "an alternate pool name." << std::endl;
    } else {
      std::cerr << "rbd: error opening pool '" << pool_name << "': "
                << cpp_strerror(r) << std::endl;
    }
    return r;
  }

  return set_namespace(namespace_name, io_ctx);
}

int set_namespace(const std::string& namespace_name, librados::IoCtx *io_ctx) {
  if (!namespace_name.empty()) {
    librbd::RBD rbd;
    bool exists = false;
    int r = rbd.namespace_exists(*io_ctx, namespace_name.c_str(), &exists);
    if (r < 0) {
      std::cerr << "rbd: error asserting namespace: "
                << cpp_strerror(r) << std::endl;
      return r;
    }
    if (!exists) {
      std::cerr << "rbd: namespace '" << namespace_name << "' does not exist."
                << std::endl;
      return -ENOENT;
    }
  }
  io_ctx->set_namespace(namespace_name);
  return 0;
}

void disable_cache() {
  g_conf().set_val_or_die("rbd_cache", "false");
}

int open_image(librados::IoCtx &io_ctx, const std::string &image_name,
               bool read_only, librbd::Image *image) {
  int r;
  librbd::RBD rbd;
  if (read_only) {
    r = rbd.open_read_only(io_ctx, *image, image_name.c_str(), NULL);
  } else {
    r = rbd.open(io_ctx, *image, image_name.c_str());
  }

  if (r < 0) {
    std::cerr << "rbd: error opening image " << image_name << ": "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

int open_image_by_id(librados::IoCtx &io_ctx, const std::string &image_id,
                     bool read_only, librbd::Image *image) {
  int r;
  librbd::RBD rbd;
  if (read_only) {
    r = rbd.open_by_id_read_only(io_ctx, *image, image_id.c_str(), NULL);
  } else {
    r = rbd.open_by_id(io_ctx, *image, image_id.c_str());
  }

  if (r < 0) {
    std::cerr << "rbd: error opening image with id " << image_id << ": "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

int init_and_open_image(const std::string &pool_name,
                        const std::string &namespace_name,
                        const std::string &image_name,
                        const std::string &image_id,
                        const std::string &snap_name, bool read_only,
                        librados::Rados *rados, librados::IoCtx *io_ctx,
                        librbd::Image *image) {
  int r = init(pool_name, namespace_name, rados, io_ctx);
  if (r < 0) {
    return r;
  }

  if (image_id.empty()) {
    r = open_image(*io_ctx, image_name, read_only, image);
  } else {
    r = open_image_by_id(*io_ctx, image_id, read_only, image);
  }
  if (r < 0) {
    return r;
  }

  if (!snap_name.empty()) {
    r = snap_set(*image, snap_name);
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

int snap_set(librbd::Image &image, const std::string &snap_name) {
  int r = image.snap_set(snap_name.c_str());
  if (r < 0) {
    std::cerr << "error setting snapshot context: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

void calc_sparse_extent(const bufferptr &bp,
                        size_t sparse_size,
			size_t buffer_offset,
                        uint64_t buffer_length,
                        size_t *write_length,
                        bool *zeroed) {
  if (sparse_size == 0) {
    // sparse writes are disabled -- write the full extent
    ceph_assert(buffer_offset == 0);
    *write_length = buffer_length;
    *zeroed = false;
    return;
  }

  *write_length = 0;
  size_t original_offset = buffer_offset;
  while (buffer_offset < buffer_length) {
    size_t extent_size = std::min<size_t>(
      sparse_size, buffer_length - buffer_offset);

    bufferptr extent(bp, buffer_offset, extent_size);

    bool extent_is_zero = extent.is_zero();
    if (original_offset == buffer_offset) {
      *zeroed = extent_is_zero;
    } else if (*zeroed != extent_is_zero) {
      ceph_assert(*write_length > 0);
      return;
    }

    buffer_offset += extent_size;
    *write_length += extent_size;
  }
}

std::string image_id(librbd::Image& image) {
  std::string id;
  int r = image.get_id(&id);
  if (r < 0) {
    return std::string();
  }
  return id;
}

std::string mirror_image_mode(librbd::mirror_image_mode_t mode) {
  switch (mode) {
    case RBD_MIRROR_IMAGE_MODE_JOURNAL:
      return "journal";
    case RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
      return "snapshot";
    default:
      return "unknown";
  }
}

std::string mirror_image_state(librbd::mirror_image_state_t state) {
  switch (state) {
    case RBD_MIRROR_IMAGE_DISABLING:
      return "disabling";
    case RBD_MIRROR_IMAGE_ENABLED:
      return "enabled";
    case RBD_MIRROR_IMAGE_DISABLED:
      return "disabled";
    default:
      return "unknown";
  }
}

std::string mirror_image_status_state(
    librbd::mirror_image_status_state_t state) {
  switch (state) {
  case MIRROR_IMAGE_STATUS_STATE_UNKNOWN:
    return "unknown";
  case MIRROR_IMAGE_STATUS_STATE_ERROR:
    return "error";
  case MIRROR_IMAGE_STATUS_STATE_SYNCING:
    return "syncing";
  case MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY:
    return "starting_replay";
  case MIRROR_IMAGE_STATUS_STATE_REPLAYING:
    return "replaying";
  case MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY:
    return "stopping_replay";
  case MIRROR_IMAGE_STATUS_STATE_STOPPED:
    return "stopped";
  default:
    return "unknown (" + stringify(static_cast<uint32_t>(state)) + ")";
  }
}

std::string mirror_image_site_status_state(
    const librbd::mirror_image_site_status_t& status) {
  return (status.up ? "up+" : "down+") +
    mirror_image_status_state(status.state);
}

std::string mirror_image_global_status_state(
    const librbd::mirror_image_global_status_t& status) {
  librbd::mirror_image_site_status_t local_status;
  int r = get_local_mirror_image_status(status, &local_status);
  if (r < 0) {
    return "down+unknown";
  }

  return mirror_image_site_status_state(local_status);
}

int get_local_mirror_image_status(
    const librbd::mirror_image_global_status_t& status,
    librbd::mirror_image_site_status_t* local_status) {
  auto it = std::find_if(status.site_statuses.begin(),
                         status.site_statuses.end(),
                         [](auto& site_status) {
      return (site_status.mirror_uuid ==
                RBD_MIRROR_IMAGE_STATUS_LOCAL_MIRROR_UUID);
    });
  if (it == status.site_statuses.end()) {
    return -ENOENT;
  }

  *local_status = *it;
  return 0;
}

std::string timestr(time_t t) {
  if (t == 0) {
    return "";
  }

  struct tm tm;

  localtime_r(&t, &tm);

  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);

  return buf;
}

uint64_t get_rbd_default_features(CephContext* cct) {
  auto features = cct->_conf.get_val<std::string>("rbd_default_features");
  return boost::lexical_cast<uint64_t>(features);
}

bool is_not_user_snap_namespace(librbd::Image* image,
                                const librbd::snap_info_t &snap_info)
{
  librbd::snap_namespace_type_t namespace_type;
  int r = image->snap_get_namespace_type(snap_info.id, &namespace_type);
  if (r < 0) {
    return false;
  }
  return namespace_type != RBD_SNAP_NAMESPACE_TYPE_USER;
}

void get_mirror_peer_sites(
    librados::IoCtx& io_ctx,
    std::vector<librbd::mirror_peer_site_t>* mirror_peers) {
  librados::IoCtx default_io_ctx;
  default_io_ctx.dup(io_ctx);
  default_io_ctx.set_namespace("");

  mirror_peers->clear();

  librbd::RBD rbd;
  int r = rbd.mirror_peer_site_list(default_io_ctx, mirror_peers);
  if (r < 0 && r != -ENOENT) {
    std::cerr << "rbd: failed to list mirror peers" << std::endl;
  }
}

void get_mirror_peer_mirror_uuids_to_names(
    const std::vector<librbd::mirror_peer_site_t>& mirror_peers,
    std::map<std::string, std::string>* mirror_uuids_to_name) {
  mirror_uuids_to_name->clear();
  for (auto& peer : mirror_peers) {
    if (!peer.mirror_uuid.empty() && !peer.site_name.empty()) {
      (*mirror_uuids_to_name)[peer.mirror_uuid] = peer.site_name;
    }
  }
}

void populate_unknown_mirror_image_site_statuses(
    const std::vector<librbd::mirror_peer_site_t>& mirror_peers,
    librbd::mirror_image_global_status_t* global_status) {
  std::set<std::string> missing_mirror_uuids;
  librbd::mirror_peer_direction_t mirror_peer_direction =
    RBD_MIRROR_PEER_DIRECTION_RX_TX;
  for (auto& peer : mirror_peers) {
    if (peer.uuid == mirror_peers.begin()->uuid) {
      mirror_peer_direction = peer.direction;
    } else if (mirror_peer_direction != RBD_MIRROR_PEER_DIRECTION_RX_TX &&
               mirror_peer_direction != peer.direction) {
      mirror_peer_direction = RBD_MIRROR_PEER_DIRECTION_RX_TX;
    }

    if (!peer.mirror_uuid.empty() &&
        peer.direction != RBD_MIRROR_PEER_DIRECTION_TX) {
      missing_mirror_uuids.insert(peer.mirror_uuid);
    }
  }

  if (mirror_peer_direction != RBD_MIRROR_PEER_DIRECTION_TX) {
    missing_mirror_uuids.insert(RBD_MIRROR_IMAGE_STATUS_LOCAL_MIRROR_UUID);
  }

  std::vector<librbd::mirror_image_site_status_t> site_statuses;
  site_statuses.reserve(missing_mirror_uuids.size());

  for (auto& site_status : global_status->site_statuses) {
    if (missing_mirror_uuids.count(site_status.mirror_uuid) > 0) {
      missing_mirror_uuids.erase(site_status.mirror_uuid);
      site_statuses.push_back(site_status);
    }
  }

  for (auto& mirror_uuid : missing_mirror_uuids) {
    site_statuses.push_back({mirror_uuid, MIRROR_IMAGE_STATUS_STATE_UNKNOWN,
                             "status not found", 0, false});
  }

  std::swap(global_status->site_statuses, site_statuses);
}

int mgr_command(librados::Rados& rados, const std::string& cmd,
                const std::map<std::string, std::string> &args,
                std::ostream *out_os, std::ostream *err_os) {
  std::string command = R"(
    {
      "prefix": ")" + cmd + R"(", )" + mgr_command_args_to_str(args) + R"(
    })";

  bufferlist in_bl;
  bufferlist out_bl;
  std::string outs;
  int r = rados.mgr_command(command, in_bl, &out_bl, &outs);
  if (r < 0) {
    (*err_os) << "rbd: " << cmd << " failed: " << cpp_strerror(r);
    if (!outs.empty()) {
      (*err_os) << ": " << outs;
    }
    (*err_os) << std::endl;
    return r;
  }

  if (out_bl.length() != 0) {
    (*out_os) << out_bl.c_str();
  }

  return 0;
}

} // namespace utils
} // namespace rbd
