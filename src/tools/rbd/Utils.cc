// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/Utils.h"
#include "include/assert.h"
#include "include/Context.h"
#include "include/encoding.h"
#include "common/common_init.h"
#include "include/stringify.h"
#include "include/rbd/features.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "global/global_context.h"
#include <iostream>
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace rbd {
namespace utils {

namespace at = argument_types;
namespace po = boost::program_options;

int ProgressContext::update_progress(uint64_t offset, uint64_t total) {
  if (progress) {
    int pc = total ? (offset * 100ull / total) : 0;
    if (pc != last_pc) {
      cerr << "\r" << operation << ": "
           << pc << "% complete...";
      cerr.flush();
      last_pc = pc;
    }
  }
  return 0;
}

void ProgressContext::finish() {
  if (progress) {
    cerr << "\r" << operation << ": 100% complete...done." << std::endl;
  }
}

void ProgressContext::fail() {
  if (progress) {
    cerr << "\r" << operation << ": " << last_pc << "% complete...failed."
         << std::endl;
  }
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
  bufferlist::iterator p = bl.begin();
  uint32_t len;
  ::decode(len, p);
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
                 std::string *image_name, std::string *snap_name,
                 SpecValidation spec_validation) {
  if (!g_ceph_context->_conf->rbd_validate_names) {
    spec_validation = SPEC_VALIDATION_NONE;
  }

  boost::regex pattern;
  switch (spec_validation) {
  case SPEC_VALIDATION_FULL:
    // disallow "/" and "@" in image and snap name
    pattern = "^(?:([^/@]+)/)?([^/@]+)(?:@([^/@]+))?$";
    break;
  case SPEC_VALIDATION_SNAP:
    // disallow "/" and "@" in snap name
    pattern = "^(?:([^/]+)/)?([^@]+)(?:@([^/@]+))?$";
    break;
  case SPEC_VALIDATION_NONE:
    // relaxed pattern assumes pool is before first "/" and snap
    // name is after first "@"
    pattern = "^(?:([^/]+)/)?([^@]+)(?:@(.+))?$";
    break;
  default:
    assert(false);
    break;
  }

  boost::smatch match;
  if (!boost::regex_match(spec, match, pattern)) {
    std::cerr << "rbd: invalid spec '" << spec << "'" << std::endl;
    return -EINVAL;
  }

  if (pool_name != nullptr && match[1].matched) {
    *pool_name = match[1];
  }
  if (image_name != nullptr) {
    *image_name = match[2];
  }
  if (snap_name != nullptr && match[3].matched) {
    *snap_name = match[3];
  }
  return 0;
}

int extract_group_spec(const std::string &spec,
		       std::string *pool_name,
		       std::string *group_name) {
  boost::regex pattern;
  pattern = "^(?:([^/]+)/)?(.+)?$";

  boost::smatch match;
  if (!boost::regex_match(spec, match, pattern)) {
    std::cerr << "rbd: invalid spec '" << spec << "'" << std::endl;
    return -EINVAL;
  }

  if (pool_name != nullptr && match[1].matched) {
    *pool_name = match[1];
  }
  if (group_name != nullptr) {
    *group_name = match[2];
  }

  return 0;
}

int extract_image_id_spec(const std::string &spec, std::string *pool_name,
                          std::string *image_id) {
  boost::regex pattern;
  pattern = "^(?:([^/]+)/)?(.+)?$";

  boost::smatch match;
  if (!boost::regex_match(spec, match, pattern)) {
    std::cerr << "rbd: invalid spec '" << spec << "'" << std::endl;
    return -EINVAL;
  }

  if (pool_name != nullptr && match[1].matched) {
    *pool_name = match[1];
  }
  if (image_id != nullptr) {
    *image_id = match[2];
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

std::string get_default_pool_name() {
  return g_ceph_context->_conf->rbd_default_pool;
}

std::string get_pool_name(const po::variables_map &vm, size_t *arg_index) {
  std::string pool_name;
  if (vm.count(at::POOL_NAME)) {
    pool_name = vm[at::POOL_NAME].as<std::string>();
  } else {
    pool_name = get_positional_argument(vm, *arg_index);
    if (!pool_name.empty()) {
       ++(*arg_index);
    }
  }

  if (pool_name.empty()) {
    pool_name = get_default_pool_name();
  }
  return pool_name;
}

int get_special_pool_group_names(const po::variables_map &vm,
				 size_t *arg_index,
				 std::string *group_pool_name,
				 std::string *group_name) {
  if (nullptr == group_pool_name) return -EINVAL;
  if (nullptr == group_name) return -EINVAL;
  std::string pool_key = at::POOL_NAME;

  std::string group_pool_key = "group-" + at::POOL_NAME;
  std::string group_key = at::GROUP_NAME;

  if (vm.count(group_pool_key)) {
    *group_pool_name = vm[group_pool_key].as<std::string>();
  }

  if (vm.count(group_key)) {
    *group_name = vm[group_key].as<std::string>();
  }

  int r;
  if (group_name->empty()) {
    std::string spec = utils::get_positional_argument(vm, (*arg_index)++);
    if (!spec.empty()) {
      r = utils::extract_group_spec(spec, group_pool_name, group_name);
      if (r < 0) {
        return r;
      }
    }
  }

  if (group_pool_name->empty() && vm.count(pool_key)) {
    *group_pool_name = vm[pool_key].as<std::string>();
  }

  if (group_pool_name->empty()) {
    *group_pool_name = get_default_pool_name();
  }

  if (group_name->empty()) {
    std::cerr << "rbd: consistency group name was not specified" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_special_pool_image_names(const po::variables_map &vm,
				 size_t *arg_index,
				 std::string *image_pool_name,
				 std::string *image_name) {
  if (nullptr == image_pool_name) return -EINVAL;
  if (nullptr == image_name) return -EINVAL;

  std::string pool_key = at::POOL_NAME;

  std::string image_pool_key = "image-" + at::POOL_NAME;
  std::string image_key = at::IMAGE_NAME;

  if (vm.count(image_pool_key)) {
    *image_pool_name = vm[image_pool_key].as<std::string>();
  }

  if (vm.count(image_key)) {
    *image_name = vm[image_key].as<std::string>();
  }

  int r;
  if (image_name->empty()) {
    std::string spec = utils::get_positional_argument(vm, (*arg_index)++);
    if (!spec.empty()) {
      r = utils::extract_spec(spec, image_pool_name,
                              image_name, nullptr,
			      utils::SPEC_VALIDATION_NONE);
      if (r < 0) {
        return r;
      }
    }
  }

  if (image_pool_name->empty() && vm.count(pool_key)) {
    *image_pool_name = vm[pool_key].as<std::string>();
  }

  if (image_pool_name->empty()) {
    *image_pool_name = get_default_pool_name();
  }

  if (image_name->empty()) {
    std::cerr << "rbd: image name was not specified" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_pool_image_id(const po::variables_map &vm,
		      size_t *spec_arg_index,
		      std::string *pool_name,
		      std::string *image_id) {

  if (vm.count(at::POOL_NAME) && pool_name != nullptr) {
    *pool_name = vm[at::POOL_NAME].as<std::string>();
  }
  if (vm.count(at::IMAGE_ID) && image_id != nullptr) {
    *image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  int r;
  if (image_id != nullptr && spec_arg_index != nullptr && image_id->empty()) {
    std::string spec = get_positional_argument(vm, (*spec_arg_index)++);
    if (!spec.empty()) {
      r = extract_image_id_spec(spec, pool_name, image_id);
      if (r < 0) {
        return r;
      }
    }
  }

  if (pool_name != nullptr && pool_name->empty()) {
    *pool_name = get_default_pool_name();
  }

  if (image_id != nullptr && image_id->empty()) {
    std::cerr << "rbd: image id was not specified" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_pool_group_names(const po::variables_map &vm,
			 at::ArgumentModifier mod,
			 size_t *spec_arg_index,
			 std::string *pool_name,
			 std::string *group_name) {
  std::string pool_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_POOL_NAME : at::POOL_NAME);
  std::string group_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_GROUP_NAME : at::GROUP_NAME);

  if (vm.count(pool_key) && pool_name != nullptr) {
    *pool_name = vm[pool_key].as<std::string>();
  }
  if (vm.count(group_key) && group_name != nullptr) {
    *group_name = vm[group_key].as<std::string>();
  }

  int r;
  if (group_name != nullptr && spec_arg_index != nullptr &&
      group_name->empty()) {
    std::string spec = get_positional_argument(vm, (*spec_arg_index)++);
    if (!spec.empty()) {
      r = extract_group_spec(spec, pool_name, group_name);
      if (r < 0) {
        return r;
      }
    }
  }

  if (pool_name != nullptr && pool_name->empty()) {
    *pool_name = get_default_pool_name();
  }

  if (group_name != nullptr && group_name->empty()) {
    std::string prefix = at::get_description_prefix(mod);
    std::cerr << "rbd: "
              << (mod == at::ARGUMENT_MODIFIER_DEST ? prefix : std::string())
              << "group name was not specified" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_pool_image_snapshot_names(const po::variables_map &vm,
                                  at::ArgumentModifier mod,
                                  size_t *spec_arg_index,
                                  std::string *pool_name,
                                  std::string *image_name,
                                  std::string *snap_name,
                                  SnapshotPresence snapshot_presence,
                                  SpecValidation spec_validation,
                                  bool image_required) {
  std::string pool_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_POOL_NAME : at::POOL_NAME);
  std::string image_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_IMAGE_NAME : at::IMAGE_NAME);
  std::string snap_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
	at::DEST_SNAPSHOT_NAME : at::SNAPSHOT_NAME);

  if (vm.count(pool_key) && pool_name != nullptr) {
    *pool_name = vm[pool_key].as<std::string>();
  }
  if (vm.count(image_key) && image_name != nullptr) {
    *image_name = vm[image_key].as<std::string>();
  }
  if (vm.count(snap_key) && snap_name != nullptr) {
     *snap_name = vm[snap_key].as<std::string>();
  }

  int r;
  if (image_name != nullptr && !image_name->empty()) {
    // despite the separate pool and snapshot name options,
    // we can also specify them via the image option
    std::string image_name_copy(*image_name);
    r = extract_spec(image_name_copy, pool_name, image_name, snap_name,
                     spec_validation);
    if (r < 0) {
      return r;
    }
  }

  if (image_name != nullptr && spec_arg_index != nullptr &&
      image_name->empty()) {
    std::string spec = get_positional_argument(vm, (*spec_arg_index)++);
    if (!spec.empty()) {
      r = extract_spec(spec, pool_name, image_name, snap_name, spec_validation);
      if (r < 0) {
        return r;
      }
    }
  }

  if (pool_name != nullptr && pool_name->empty()) {
    *pool_name = get_default_pool_name();
  }

  if (image_name != nullptr && image_required && image_name->empty()) {
    std::string prefix = at::get_description_prefix(mod);
    std::cerr << "rbd: "
              << (mod == at::ARGUMENT_MODIFIER_DEST ? prefix : std::string())
              << "image name was not specified" << std::endl;
    return -EINVAL;
  }

  //Validate pool name while creating/renaming/copying/cloning/importing/etc
  if (spec_validation == SPEC_VALIDATION_FULL) {
    boost::regex pattern("^[^@/]+?$");
    if (!boost::regex_match (*pool_name, pattern)) {
      std::cerr << "rbd: invalid pool name '" << *pool_name << "'" << std::endl;
      return -EINVAL;
    }
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

int get_pool_snapshot_names(const po::variables_map &vm,
                            at::ArgumentModifier mod,
                            size_t *spec_arg_index,
                            std::string *pool_name,
                            std::string *snap_name,
                            SnapshotPresence snapshot_presence,
                            SpecValidation spec_validation) {
  std::string pool_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_POOL_NAME : at::POOL_NAME);
  std::string snap_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
	at::DEST_SNAPSHOT_NAME : at::SNAPSHOT_NAME);

  if (vm.count(pool_key) && pool_name != nullptr) {
    *pool_name = vm[pool_key].as<std::string>();
  }
  if (vm.count(snap_key) && snap_name != nullptr) {
     *snap_name = vm[snap_key].as<std::string>();
  }

  if (pool_name != nullptr && pool_name->empty()) {
    *pool_name = get_default_pool_name();
  }

  if (snap_name != nullptr) {
    int r = validate_snapshot_name(mod, *snap_name, snapshot_presence,
                                   spec_validation);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

int get_pool_journal_names(const po::variables_map &vm,
			   at::ArgumentModifier mod,
			   size_t *spec_arg_index,
			   std::string *pool_name,
			   std::string *journal_name) {
  std::string pool_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_POOL_NAME : at::POOL_NAME);
  std::string image_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_IMAGE_NAME : at::IMAGE_NAME);
  std::string journal_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_JOURNAL_NAME : at::JOURNAL_NAME);

  if (vm.count(pool_key) && pool_name != nullptr) {
    *pool_name = vm[pool_key].as<std::string>();
  }
  if (vm.count(journal_key) && journal_name != nullptr) {
    *journal_name = vm[journal_key].as<std::string>();
  }

  std::string image_name;
  if (vm.count(image_key)) {
    image_name = vm[image_key].as<std::string>();
  }

  int r;
  if (journal_name != nullptr && !journal_name->empty()) {
    // despite the separate pool option,
    // we can also specify them via the journal option
    std::string journal_name_copy(*journal_name);
    r = extract_spec(journal_name_copy, pool_name, journal_name, nullptr,
                     SPEC_VALIDATION_FULL);
    if (r < 0) {
      return r;
    }
  }

  if (!image_name.empty()) {
    // despite the separate pool option,
    // we can also specify them via the image option
    std::string image_name_copy(image_name);
    r = extract_spec(image_name_copy, pool_name, &image_name, nullptr,
                     SPEC_VALIDATION_NONE);
    if (r < 0) {
      return r;
    }
  }

  if (journal_name != nullptr && spec_arg_index != nullptr &&
      journal_name->empty()) {
    std::string spec = get_positional_argument(vm, (*spec_arg_index)++);
    if (!spec.empty()) {
      r = extract_spec(spec, pool_name, journal_name, nullptr,
                       SPEC_VALIDATION_FULL);
      if (r < 0) {
        return r;
      }
    }
  }

  if (pool_name != nullptr && pool_name->empty()) {
    *pool_name = get_default_pool_name();
  }

  if (pool_name != nullptr && journal_name != nullptr &&
      journal_name->empty() && !image_name.empty()) {
    // Try to get journal name from image info.
    librados::Rados rados;
    librados::IoCtx io_ctx;
    librbd::Image image;
    int r = init_and_open_image(*pool_name, image_name, "", "", true, &rados,
                                &io_ctx, &image);
    if (r < 0) {
      std::cerr << "rbd: failed to open image " << image_name
		<< " to get journal name: " << cpp_strerror(r) << std::endl;
      return r;
    }

    uint64_t features;
    r = image.features(&features);
    if (r < 0) {
      return r;
    }
    if ((features & RBD_FEATURE_JOURNALING) == 0) {
      std::cerr << "rbd: journaling is not enabled for image " << image_name
		<< std::endl;
      return -EINVAL;
    }
    *journal_name = image_id(image);
  }

  if (journal_name != nullptr && journal_name->empty()) {
    std::string prefix = at::get_description_prefix(mod);
    std::cerr << "rbd: "
              << (mod == at::ARGUMENT_MODIFIER_DEST ? prefix : std::string())
              << "journal was not specified" << std::endl;
    return -EINVAL;
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
                << "snapname specified for a command that doesn't use it"
                << std::endl;
      return -EINVAL;
    }
    break;
  case SNAPSHOT_PRESENCE_REQUIRED:
    if (snap_name.empty()) {
      std::cerr << "rbd: "
                << (mod == at::ARGUMENT_MODIFIER_DEST ? prefix : std::string())
                << "snap name was not specified" << std::endl;
      return -EINVAL;
    }
    break;
  }

  if (spec_validation == SPEC_VALIDATION_SNAP) {
    // disallow "/" and "@" in snap name
    boost::regex pattern("^[^@/]*?$");
    if (!boost::regex_match (snap_name, pattern)) {
      std::cerr << "rbd: invalid snap name '" << snap_name << "'" << std::endl;
      return -EINVAL;
    }
  }
  return 0;
}

int get_image_options(const boost::program_options::variables_map &vm,
		      bool get_format, librbd::ImageOptions *opts) {
  uint64_t order = 0, stripe_unit = 0, stripe_count = 0, object_size = 0;
  uint64_t features = 0, features_clear = 0, features_set = 0;
  std::string data_pool;
  bool order_specified = true;
  bool features_specified = false;
  bool features_clear_specified = false;
  bool features_set_specified = false;
  bool stripe_specified = false;

  if (vm.count(at::IMAGE_ORDER)) {
    order = vm[at::IMAGE_ORDER].as<uint64_t>();
    std::cerr << "rbd: --order is deprecated, use --object-size"
	      << std::endl;
  } else if (vm.count(at::IMAGE_OBJECT_SIZE)) {
    object_size = vm[at::IMAGE_OBJECT_SIZE].as<uint64_t>();
    order = std::round(std::log2(object_size)); 
  } else {
    order_specified = false;
  }

  if (vm.count(at::IMAGE_FEATURES)) {
    features = vm[at::IMAGE_FEATURES].as<uint64_t>();
    features_specified = true;
  } else {
    features = get_rbd_default_features(g_ceph_context);
  }

  if (vm.count(at::IMAGE_STRIPE_UNIT)) {
    stripe_unit = vm[at::IMAGE_STRIPE_UNIT].as<uint64_t>();
    stripe_specified = true;
  }

  if (vm.count(at::IMAGE_STRIPE_COUNT)) {
    stripe_count = vm[at::IMAGE_STRIPE_COUNT].as<uint64_t>();
    stripe_specified = true;
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
      int r = g_conf->set_val("rbd_default_format", stringify(format));
      assert(r == 0);
      opts->set(RBD_IMAGE_OPTION_FORMAT, format);
    }
  }

  if (order_specified)
    opts->set(RBD_IMAGE_OPTION_ORDER, order);
  if (features_specified)
    opts->set(RBD_IMAGE_OPTION_FEATURES, features);
  if (features_clear_specified) {
    opts->set(RBD_IMAGE_OPTION_FEATURES_CLEAR, features_clear);
  }
  if (features_set_specified)
    opts->set(RBD_IMAGE_OPTION_FEATURES_SET, features_set);
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

    int r = g_conf->set_val("rbd_journal_order", stringify(order));
    assert(r == 0);
  }
  if (vm.count(at::JOURNAL_SPLAY_WIDTH)) {
    opts->set(RBD_IMAGE_OPTION_JOURNAL_SPLAY_WIDTH,
	      vm[at::JOURNAL_SPLAY_WIDTH].as<uint64_t>());

    int r = g_conf->set_val("rbd_journal_splay_width",
			    stringify(
			      vm[at::JOURNAL_SPLAY_WIDTH].as<uint64_t>()));
    assert(r == 0);
  }
  if (vm.count(at::JOURNAL_POOL)) {
    opts->set(RBD_IMAGE_OPTION_JOURNAL_POOL,
	      vm[at::JOURNAL_POOL].as<std::string>());

    int r = g_conf->set_val("rbd_journal_pool",
			    vm[at::JOURNAL_POOL].as<std::string>());
    assert(r == 0);
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
             const std::string &positional_path, std::string *path) {
  if (!positional_path.empty()) {
    *path = positional_path;
  } else if (vm.count(at::PATH)) {
    *path = vm[at::PATH].as<std::string>();
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
    }
  }
  return 0;
}

void init_context() {
  g_conf->set_val_or_die("rbd_cache_writethrough_until_flush", "false");
  g_conf->apply_changes(NULL);
  common_init_finish(g_ceph_context);
}

int init(const std::string &pool_name, librados::Rados *rados,
         librados::IoCtx *io_ctx) {
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

  r = init_io_ctx(*rados, pool_name, io_ctx);
  if (r < 0) {
    return r;
  }
  return 0;
}

int init_io_ctx(librados::Rados &rados, const std::string &pool_name,
                librados::IoCtx *io_ctx) {
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
  return 0;
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
                        const std::string &image_name,
                        const std::string &image_id,
                        const std::string &snap_name, bool read_only,
                        librados::Rados *rados, librados::IoCtx *io_ctx,
                        librbd::Image *image) {
  int r = init(pool_name, rados, io_ctx);
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
    assert(buffer_offset == 0);
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
      assert(*write_length > 0);
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

std::string mirror_image_status_state(librbd::mirror_image_status_state_t state) {
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

std::string mirror_image_status_state(librbd::mirror_image_status_t status) {
  return (status.up ? "up+" : "down+") +
    mirror_image_status_state(status.state);
}

std::string timestr(time_t t) {
  struct tm tm;

  localtime_r(&t, &tm);

  char buf[32];
  strftime(buf, sizeof(buf), "%F %T", &tm);

  return buf;
}

uint64_t get_rbd_default_features(CephContext* cct) {
  auto features = cct->_conf->get_val<std::string>("rbd_default_features");
  return boost::lexical_cast<uint64_t>(features);
}

bool check_if_image_spec_present(const po::variables_map &vm,
                                 at::ArgumentModifier mod,
                                 size_t spec_arg_index) {
  std::string image_key = (mod == at::ARGUMENT_MODIFIER_DEST ?
    at::DEST_IMAGE_NAME : at::IMAGE_NAME);

  if (vm.count(image_key)) {
    return true;
  }

  std::string spec = get_positional_argument(vm, spec_arg_index);
  if (!spec.empty()) {
    return true;
  }

  return false;
}

} // namespace utils
} // namespace rbd
