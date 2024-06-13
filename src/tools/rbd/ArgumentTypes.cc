// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rbd/features.h"
#include "common/config_proxy.h"
#include "common/strtol.h"
#include "common/Formatter.h"
#include "global/global_context.h"
#include <iostream>
#include <boost/tokenizer.hpp>

namespace rbd {
namespace argument_types {

namespace po = boost::program_options;

const std::map<uint64_t, std::string> ImageFeatures::FEATURE_MAPPING = {
  {RBD_FEATURE_LAYERING, RBD_FEATURE_NAME_LAYERING},
  {RBD_FEATURE_STRIPINGV2, RBD_FEATURE_NAME_STRIPINGV2},
  {RBD_FEATURE_EXCLUSIVE_LOCK, RBD_FEATURE_NAME_EXCLUSIVE_LOCK},
  {RBD_FEATURE_OBJECT_MAP, RBD_FEATURE_NAME_OBJECT_MAP},
  {RBD_FEATURE_FAST_DIFF, RBD_FEATURE_NAME_FAST_DIFF},
  {RBD_FEATURE_DEEP_FLATTEN, RBD_FEATURE_NAME_DEEP_FLATTEN},
  {RBD_FEATURE_JOURNALING, RBD_FEATURE_NAME_JOURNALING},
  {RBD_FEATURE_DATA_POOL, RBD_FEATURE_NAME_DATA_POOL},
  {RBD_FEATURE_OPERATIONS, RBD_FEATURE_NAME_OPERATIONS},
  {RBD_FEATURE_MIGRATING, RBD_FEATURE_NAME_MIGRATING},
  {RBD_FEATURE_NON_PRIMARY, RBD_FEATURE_NAME_NON_PRIMARY},
  {RBD_FEATURE_DIRTY_CACHE, RBD_FEATURE_NAME_DIRTY_CACHE},
};

Format::Formatter Format::create_formatter(bool pretty) const {
  if (value == "json") {
    return Formatter(new JSONFormatter(pretty));
  } else if (value == "xml") {
    return Formatter(new XMLFormatter(pretty));
  }
  return Formatter();
}

std::string get_name_prefix(ArgumentModifier modifier) {
  switch (modifier) {
  case ARGUMENT_MODIFIER_SOURCE:
    return SOURCE_PREFIX;
  case ARGUMENT_MODIFIER_DEST:
    return DEST_PREFIX;
  default:
    return "";
  }
}

std::string get_description_prefix(ArgumentModifier modifier) {
  switch (modifier) {
  case ARGUMENT_MODIFIER_SOURCE:
    return "source ";
  case ARGUMENT_MODIFIER_DEST:
    return "destination ";
  default:
    return "";
  }
}

void add_pool_option(po::options_description *opt,
                     ArgumentModifier modifier,
                     const std::string &desc_suffix) {
  std::string name = POOL_NAME + ",p";
  std::string description = "pool name";
  switch (modifier) {
  case ARGUMENT_MODIFIER_NONE:
    break;
  case ARGUMENT_MODIFIER_SOURCE:
    description = "source " + description;
    break;
  case ARGUMENT_MODIFIER_DEST:
    name = DEST_POOL_NAME;
    description = "destination " + description;
    break;
  }
  description += desc_suffix;

  // TODO add validator
  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_namespace_option(boost::program_options::options_description *opt,
                          ArgumentModifier modifier) {
  std::string name = NAMESPACE_NAME;
  std::string description = "namespace name";
  switch (modifier) {
  case ARGUMENT_MODIFIER_NONE:
    break;
  case ARGUMENT_MODIFIER_SOURCE:
    description = "source " + description;
    break;
  case ARGUMENT_MODIFIER_DEST:
    name = DEST_NAMESPACE_NAME;
    description = "destination " + description;
    break;
  }

  // TODO add validator
  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_image_option(po::options_description *opt,
                      ArgumentModifier modifier,
                      const std::string &desc_suffix) {
  std::string name = IMAGE_NAME;
  std::string description = "image name";
  switch (modifier) {
  case ARGUMENT_MODIFIER_NONE:
    break;
  case ARGUMENT_MODIFIER_SOURCE:
    description = "source " + description;
    break;
  case ARGUMENT_MODIFIER_DEST:
    name = DEST_IMAGE_NAME;
    description = "destination " + description;
    break;
  }
  description += desc_suffix;

  // TODO add validator
  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_image_id_option(po::options_description *opt,
                         const std::string &desc_suffix) {
  std::string name = IMAGE_ID;
  std::string description = "image id";
  description += desc_suffix;

  // TODO add validator
  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_snap_option(po::options_description *opt,
                      ArgumentModifier modifier) {

  std::string name = SNAPSHOT_NAME;
  std::string description = "snapshot name";
  switch (modifier) {
  case ARGUMENT_MODIFIER_NONE:
    break;
  case ARGUMENT_MODIFIER_DEST:
    name = DEST_SNAPSHOT_NAME;
    description = "destination " + description;
    break;
  case ARGUMENT_MODIFIER_SOURCE:
    description = "source " + description;
    break;
  }

  // TODO add validator
  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_snap_id_option(po::options_description *opt,
                        ArgumentModifier modifier) {
  std::string name = SNAPSHOT_ID;
  std::string description = "snapshot id";
  switch (modifier) {
  case ARGUMENT_MODIFIER_NONE:
  case ARGUMENT_MODIFIER_DEST:
    break;
  case ARGUMENT_MODIFIER_SOURCE:
    description = "source " + description;
    break;
  }

  opt->add_options()
    (name.c_str(), po::value<uint64_t>(), description.c_str());
}

void add_pool_options(boost::program_options::options_description *pos,
                      boost::program_options::options_description *opt,
                      bool namespaces_supported) {
  opt->add_options()
    ((POOL_NAME + ",p").c_str(), po::value<std::string>(), "pool name");
  if (namespaces_supported) {
    add_namespace_option(opt, ARGUMENT_MODIFIER_NONE);
    pos->add_options()
      ("pool-spec", "pool specification\n"
       "(example: <pool-name>[/<namespace>]");
  } else {
    pos->add_options()
      ("pool-name", "pool name");
  }
}

void add_image_spec_options(po::options_description *pos,
                            po::options_description *opt,
                            ArgumentModifier modifier) {
  pos->add_options()
    ((get_name_prefix(modifier) + IMAGE_SPEC).c_str(),
     (get_description_prefix(modifier) + "image specification\n" +
      "(example: [<pool-name>/[<namespace>/]]<image-name>)").c_str());
  add_pool_option(opt, modifier);
  add_namespace_option(opt, modifier);
  add_image_option(opt, modifier);
}

void add_snap_spec_options(po::options_description *pos,
                           po::options_description *opt,
                           ArgumentModifier modifier) {
  pos->add_options()
    ((get_name_prefix(modifier) + SNAPSHOT_SPEC).c_str(),
     (get_description_prefix(modifier) + "snapshot specification\n" +
      "(example: [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)").c_str());
  add_pool_option(opt, modifier);
  add_namespace_option(opt, modifier);
  add_image_option(opt, modifier);
  add_snap_option(opt, modifier);
}

void add_image_or_snap_spec_options(po::options_description *pos,
                                    po::options_description *opt,
                                    ArgumentModifier modifier) {
  pos->add_options()
    ((get_name_prefix(modifier) + IMAGE_OR_SNAPSHOT_SPEC).c_str(),
     (get_description_prefix(modifier) + "image or snapshot specification\n" +
      "(example: [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>])").c_str());
  add_pool_option(opt, modifier);
  add_namespace_option(opt, modifier);
  add_image_option(opt, modifier);
  add_snap_option(opt, modifier);
}

void add_create_image_options(po::options_description *opt,
                              bool include_format) {
  // TODO get default image format from conf
  if (include_format) {
    opt->add_options()
      (IMAGE_FORMAT.c_str(), po::value<ImageFormat>(),
       "image format [default: 2]")
      (IMAGE_NEW_FORMAT.c_str(),
       po::value<ImageNewFormat>()->zero_tokens(),
       "deprecated[:image-format 2]");
  }

  opt->add_options()
    (IMAGE_ORDER.c_str(), po::value<ImageOrder>(),
     "deprecated[:object-size]")
    (IMAGE_OBJECT_SIZE.c_str(), po::value<ImageObjectSize>(),
     "object size in B/K/M [4K <= object size <= 32M]")
    (IMAGE_FEATURES.c_str(), po::value<ImageFeatures>()->composing(),
     ("image features\n" + get_short_features_help(true)).c_str())
    (IMAGE_SHARED.c_str(), po::bool_switch(), "shared image")
    (IMAGE_STRIPE_UNIT.c_str(), po::value<ImageObjectSize>(), "stripe unit in B/K/M")
    (IMAGE_STRIPE_COUNT.c_str(), po::value<uint64_t>(), "stripe count")
    (IMAGE_DATA_POOL.c_str(), po::value<std::string>(), "data pool")
    (IMAGE_MIRROR_IMAGE_MODE.c_str(), po::value<MirrorImageMode>(),
     "mirror image mode [journal or snapshot]");

  add_create_journal_options(opt);
}

void add_create_journal_options(po::options_description *opt) {
  opt->add_options()
    (JOURNAL_SPLAY_WIDTH.c_str(), po::value<uint64_t>(),
     "number of active journal objects")
    (JOURNAL_OBJECT_SIZE.c_str(), po::value<JournalObjectSize>(),
     "size of journal objects [4K <= size <= 64M]")
    (JOURNAL_POOL.c_str(), po::value<std::string>(),
     "pool for journal objects");
}

void add_size_option(boost::program_options::options_description *opt) {
  opt->add_options()
    ((IMAGE_SIZE + ",s").c_str(), po::value<ImageSize>()->required(),
     "image size (in M/G/T) [default: M]");
}

void add_sparse_size_option(boost::program_options::options_description *opt) {
  opt->add_options()
    (IMAGE_SPARSE_SIZE.c_str(), po::value<ImageObjectSize>(),
    "sparse size in B/K/M [default: 4K]");
}

void add_path_options(boost::program_options::options_description *pos,
                      boost::program_options::options_description *opt,
                      const std::string &description) {
  pos->add_options()
    (PATH_NAME.c_str(), po::value<std::string>(), description.c_str());
  opt->add_options()
    (PATH.c_str(), po::value<std::string>(), description.c_str());
}

void add_limit_option(po::options_description *opt) {
  std::string description = "maximum allowed snapshot count";

  opt->add_options()
    (LIMIT.c_str(), po::value<uint64_t>(), description.c_str());
}

void add_no_progress_option(boost::program_options::options_description *opt) {
  opt->add_options()
    (NO_PROGRESS.c_str(), po::bool_switch(), "disable progress output");
}

void add_format_options(boost::program_options::options_description *opt) {
  opt->add_options()
    (FORMAT.c_str(), po::value<Format>(), "output format (plain, json, or xml) [default: plain]")
    (PRETTY_FORMAT.c_str(), po::bool_switch(),
     "pretty formatting (json and xml)");
}

void add_verbose_option(boost::program_options::options_description *opt) {
  opt->add_options()
    (VERBOSE.c_str(), po::bool_switch(), "be verbose");
}

void add_no_error_option(boost::program_options::options_description *opt) {
  opt->add_options()
    (NO_ERR.c_str(), po::bool_switch(), "continue after error");
}

void add_export_format_option(boost::program_options::options_description *opt) {
  opt->add_options()
    ("export-format", po::value<ExportFormat>(), "format of image file");
}

void add_flatten_option(boost::program_options::options_description *opt) {
  opt->add_options()
    (IMAGE_FLATTEN.c_str(), po::bool_switch(),
     "fill clone with parent data (make it independent)");
}

void add_snap_create_options(po::options_description *opt) {
  opt->add_options()
    (SKIP_QUIESCE.c_str(), po::bool_switch(), "do not run quiesce hooks")
    (IGNORE_QUIESCE_ERROR.c_str(), po::bool_switch(),
     "ignore quiesce hook error");
}

void add_encryption_options(boost::program_options::options_description *opt) {
  opt->add_options()
    (ENCRYPTION_FORMAT.c_str(),
     po::value<std::vector<EncryptionFormat>>(),
     "encryption format (luks, luks1, luks2) [default: luks]");

  opt->add_options()
    (ENCRYPTION_PASSPHRASE_FILE.c_str(),
     po::value<std::vector<std::string>>(),
     "path to file containing passphrase for unlocking the image");
}

std::string get_short_features_help(bool append_suffix) {
  std::ostringstream oss;
  bool first_feature = true;
  oss << "[";
  for (auto &pair : ImageFeatures::FEATURE_MAPPING) {
    if ((pair.first & RBD_FEATURES_IMPLICIT_ENABLE) != 0ULL) {
      // hide implicitly enabled features from list
      continue;
    } else if (!append_suffix && (pair.first & RBD_FEATURES_MUTABLE) == 0ULL) {
      // hide non-mutable features for the 'rbd feature XYZ' command
      continue;
    }

    if (!first_feature) {
      oss << ", ";
    }
    first_feature = false;

    std::string suffix;
    if (append_suffix) {
      if ((pair.first & rbd::utils::get_rbd_default_features(g_ceph_context)) != 0) {
        suffix += "+";
      }
      if ((pair.first & RBD_FEATURES_MUTABLE) != 0) {
        suffix += "*";
      } else if ((pair.first & RBD_FEATURES_DISABLE_ONLY) != 0) {
        suffix += "-";
      }
      if (!suffix.empty()) {
        suffix = "(" + suffix + ")";
      }
    }
    oss << pair.second << suffix;
  }
  oss << "]";
  return oss.str();
}

std::string get_long_features_help() {
  std::ostringstream oss;
  oss << "Image Features:" << std::endl
      << "  (*) supports enabling/disabling on existing images" << std::endl
      << "  (-) supports disabling-only on existing images" << std::endl
      << "  (+) enabled by default for new images if features not specified"
      << std::endl;
  return oss.str();
}

void validate(boost::any& v, const std::vector<std::string>& values,
              ImageSize *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t size = strict_iecstrtoll(s, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }

  //NOTE: We can remove below given three lines of code once all applications,
  //which use this CLI will adopt B/K/M/G/T/P/E with size value
  if (isdigit(*s.rbegin())) {
    size = size << 20;   // Default MB to Bytes
  }
  v = boost::any(size);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              ImageOrder *target_type, int dummy) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  try {
    uint64_t order = boost::lexical_cast<uint64_t>(s);
    if (order >= 12 && order <= 25) {
      v = boost::any(order);
      return;
    }
  } catch (const boost::bad_lexical_cast &) {
  }
  throw po::validation_error(po::validation_error::invalid_option_value);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              ImageObjectSize *target_type, int dummy) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t objectsize = strict_iecstrtoll(s, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(objectsize);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              ImageFormat *target_type, int dummy) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  try {
    uint32_t format = boost::lexical_cast<uint32_t>(s);
    if (format == 1 || format == 2) {
      v = boost::any(format);
      return;
    }
  } catch (const boost::bad_lexical_cast &) {
  }
  throw po::validation_error(po::validation_error::invalid_option_value);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              ImageNewFormat *target_type, int dummy) {
  v = boost::any(true);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              ImageFeatures *target_type, int) {
  if (v.empty()) {
    v = boost::any(static_cast<uint64_t>(0));
  }

  uint64_t &features = boost::any_cast<uint64_t &>(v);
  for (auto &value : values) {
    boost::char_separator<char> sep(",");
    boost::tokenizer<boost::char_separator<char> > tok(value, sep);
    for (auto &token : tok) {
      bool matched = false;
      for (auto &it : ImageFeatures::FEATURE_MAPPING) {
        if (token == it.second) {
          features |= it.first;
          matched = true;
          break;
        }
      }

      if (!matched) {
        throw po::validation_error(po::validation_error::invalid_option_value);
      }
    }
  }
}

void validate(boost::any& v, const std::vector<std::string>& values,
              MirrorImageMode* mirror_image_mode, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "journal") {
    v = boost::any(RBD_MIRROR_IMAGE_MODE_JOURNAL);
  } else if (s == "snapshot") {
    v = boost::any(RBD_MIRROR_IMAGE_MODE_SNAPSHOT);
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void validate(boost::any& v, const std::vector<std::string>& values,
              Format *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "plain" || s == "json" || s == "xml") {
    v = boost::any(Format(s));
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void validate(boost::any& v, const std::vector<std::string>& values,
              JournalObjectSize *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t size = strict_iecstrtoll(s, &parse_error);
  if (parse_error.empty() && (size >= (1 << 12)) && (size <= (1 << 26))) {
    v = boost::any(size);
    return;
  }
  throw po::validation_error(po::validation_error::invalid_option_value);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              EncryptionAlgorithm *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "aes-128") {
    v = boost::any(RBD_ENCRYPTION_ALGORITHM_AES128);
  } else if (s == "aes-256") {
    v = boost::any(RBD_ENCRYPTION_ALGORITHM_AES256);
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void validate(boost::any& v, const std::vector<std::string>& values,
              EncryptionFormat *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "luks") {
    v = boost::any(EncryptionFormat{RBD_ENCRYPTION_FORMAT_LUKS});
  } else if (s == "luks1") {
    v = boost::any(EncryptionFormat{RBD_ENCRYPTION_FORMAT_LUKS1});
  } else if (s == "luks2") {
    v = boost::any(EncryptionFormat{RBD_ENCRYPTION_FORMAT_LUKS2});
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void validate(boost::any& v, const std::vector<std::string>& values,
              ExportFormat *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t format = strict_iecstrtoll(s, &parse_error);
  if (!parse_error.empty() || (format != 1 && format != 2)) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }

  v = boost::any(format);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              Secret *target_type, int) {

  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  g_conf().set_val_or_die("keyfile", s.c_str());
  v = boost::any(s);
}

} // namespace argument_types
} // namespace rbd
