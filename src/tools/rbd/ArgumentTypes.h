// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_ARGUMENT_TYPES_H
#define CEPH_RBD_ARGUMENT_TYPES_H

#include "include/int_types.h"
#include <set>
#include <string>
#include <vector>
#include <boost/any.hpp>
#include <boost/program_options.hpp>
#include <boost/shared_ptr.hpp>

namespace ceph { class Formatter; }

namespace rbd {
namespace argument_types {

enum ArgumentModifier {
  ARGUMENT_MODIFIER_NONE,
  ARGUMENT_MODIFIER_SOURCE,
  ARGUMENT_MODIFIER_DEST
};

enum SpecFormat {
  SPEC_FORMAT_IMAGE,
  SPEC_FORMAT_SNAPSHOT,
  SPEC_FORMAT_IMAGE_OR_SNAPSHOT
};

static const std::string SOURCE_PREFIX("source-");
static const std::string DEST_PREFIX("dest-");

// positional arguments
static const std::string POSITIONAL_COMMAND_SPEC("positional-command-spec");
static const std::string POSITIONAL_ARGUMENTS("positional-arguments");
static const std::string IMAGE_SPEC("image-spec");
static const std::string SNAPSHOT_SPEC("snap-spec");
static const std::string IMAGE_OR_SNAPSHOT_SPEC("image-or-snap-spec");
static const std::string PATH_NAME("path-name");
static const std::string IMAGE_ID("image-id");

// optional arguments
static const std::string CONFIG_PATH("conf");
static const std::string POOL_NAME("pool");
static const std::string DEST_POOL_NAME("dest-pool");
static const std::string NAMESPACE_NAME("namespace");
static const std::string DEST_NAMESPACE_NAME("dest-namespace");
static const std::string IMAGE_NAME("image");
static const std::string DEST_IMAGE_NAME("dest");
static const std::string SNAPSHOT_NAME("snap");
static const std::string SNAPSHOT_ID("snap-id");
static const std::string DEST_SNAPSHOT_NAME("dest-snap");
static const std::string PATH("path");
static const std::string FROM_SNAPSHOT_NAME("from-snap");
static const std::string WHOLE_OBJECT("whole-object");

// encryption arguments
static const std::string ENCRYPTION_FORMAT("encryption-format");
static const std::string ENCRYPTION_PASSPHRASE_FILE("encryption-passphrase-file");

static const std::string IMAGE_FORMAT("image-format");
static const std::string IMAGE_NEW_FORMAT("new-format");
static const std::string IMAGE_ORDER("order");
static const std::string IMAGE_OBJECT_SIZE("object-size");
static const std::string IMAGE_FEATURES("image-feature");
static const std::string IMAGE_SHARED("image-shared");
static const std::string IMAGE_SIZE("size");
static const std::string IMAGE_STRIPE_UNIT("stripe-unit");
static const std::string IMAGE_STRIPE_COUNT("stripe-count");
static const std::string IMAGE_DATA_POOL("data-pool");
static const std::string IMAGE_SPARSE_SIZE("sparse-size");
static const std::string IMAGE_THICK_PROVISION("thick-provision");
static const std::string IMAGE_FLATTEN("flatten");
static const std::string IMAGE_MIRROR_IMAGE_MODE("mirror-image-mode");

static const std::string JOURNAL_OBJECT_SIZE("journal-object-size");
static const std::string JOURNAL_SPLAY_WIDTH("journal-splay-width");
static const std::string JOURNAL_POOL("journal-pool");

static const std::string NO_PROGRESS("no-progress");
static const std::string FORMAT("format");
static const std::string PRETTY_FORMAT("pretty-format");
static const std::string VERBOSE("verbose");
static const std::string NO_ERR("no-error");

static const std::string LIMIT("limit");

static const std::string SKIP_QUIESCE("skip-quiesce");
static const std::string IGNORE_QUIESCE_ERROR("ignore-quiesce-error");

static const std::string EXCLUDE_PARENT("exclude-parent");
static const std::string SKIP_CREATE("skip-create");

static const std::set<std::string> SWITCH_ARGUMENTS = {
  WHOLE_OBJECT, IMAGE_SHARED, IMAGE_THICK_PROVISION, IMAGE_FLATTEN,
  NO_PROGRESS, PRETTY_FORMAT, VERBOSE, NO_ERR, SKIP_QUIESCE,
  IGNORE_QUIESCE_ERROR, EXCLUDE_PARENT, SKIP_CREATE
};

struct ImageSize {};
struct ImageOrder {};
struct ImageObjectSize {};
struct ImageFormat {};
struct ImageNewFormat {};

struct ImageFeatures {
  static const std::map<uint64_t, std::string>  FEATURE_MAPPING;

  uint64_t features;
};

struct MirrorImageMode {};

template <typename T>
struct TypedValue {
  T value;
  TypedValue(const T& t) : value(t) {}
};

struct Format : public TypedValue<std::string> {
  typedef boost::shared_ptr<ceph::Formatter> Formatter;

  Format(const std::string &format) : TypedValue<std::string>(format) {}

  Formatter create_formatter(bool pretty) const;
};

struct JournalObjectSize {};

struct ExportFormat {};

struct Secret {};

struct EncryptionAlgorithm {};
struct EncryptionFormat {
  uint64_t format;
};

void add_export_format_option(boost::program_options::options_description *opt);

std::string get_name_prefix(ArgumentModifier modifier);
std::string get_description_prefix(ArgumentModifier modifier);

void add_all_option(boost::program_options::options_description *opt,
		    std::string description);

void add_pool_option(boost::program_options::options_description *opt,
                     ArgumentModifier modifier,
                     const std::string &desc_suffix = "");
void add_namespace_option(boost::program_options::options_description *opt,
                          ArgumentModifier modifier);

void add_image_option(boost::program_options::options_description *opt,
                      ArgumentModifier modifier,
                      const std::string &desc_suffix = "");

void add_image_id_option(boost::program_options::options_description *opt,
                         const std::string &desc_suffix = "");

void add_snap_option(boost::program_options::options_description *opt,
                     ArgumentModifier modifier);
void add_snap_id_option(boost::program_options::options_description *opt);

void add_pool_options(boost::program_options::options_description *pos,
                      boost::program_options::options_description *opt,
                      bool namespaces_supported);

void add_image_spec_options(boost::program_options::options_description *pos,
                            boost::program_options::options_description *opt,
                            ArgumentModifier modifier);

void add_snap_spec_options(boost::program_options::options_description *pos,
                           boost::program_options::options_description *opt,
                           ArgumentModifier modifier);

void add_image_or_snap_spec_options(
  boost::program_options::options_description *pos,
  boost::program_options::options_description *opt,
  ArgumentModifier modifier);

void add_create_image_options(boost::program_options::options_description *opt,
                              bool include_format);

void add_create_journal_options(
  boost::program_options::options_description *opt);

void add_size_option(boost::program_options::options_description *opt);

void add_sparse_size_option(boost::program_options::options_description *opt);

void add_path_options(boost::program_options::options_description *pos,
                      boost::program_options::options_description *opt,
                      const std::string &description);

void add_limit_option(boost::program_options::options_description *opt);

void add_no_progress_option(boost::program_options::options_description *opt);

void add_exclude_parent_option(boost::program_options::options_description *opt);

void add_skip_create_option(boost::program_options::options_description *opt);

void add_format_options(boost::program_options::options_description *opt);

void add_verbose_option(boost::program_options::options_description *opt);

void add_no_error_option(boost::program_options::options_description *opt);

void add_flatten_option(boost::program_options::options_description *opt);

void add_snap_create_options(boost::program_options::options_description *opt);

void add_encryption_options(boost::program_options::options_description *opt);

std::string get_short_features_help(bool append_suffix);
std::string get_long_features_help();

void validate(boost::any& v, const std::vector<std::string>& values,
              ExportFormat *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              ImageSize *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              ImageOrder *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              ImageObjectSize *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              ImageFormat *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              ImageNewFormat *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              ImageFeatures *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              Format *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              JournalObjectSize *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              EncryptionAlgorithm *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              EncryptionFormat *target_type, int);
void validate(boost::any& v, const std::vector<std::string>& values,
              Secret *target_type, int);


std::ostream &operator<<(std::ostream &os, const ImageFeatures &features);

} // namespace argument_types
} // namespace rbd

#endif // CEPH_RBD_ARGUMENT_TYPES_H
