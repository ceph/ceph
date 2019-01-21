// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_UTILS_H
#define CEPH_RBD_UTILS_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "tools/rbd/ArgumentTypes.h"
#include <string>
#include <boost/program_options.hpp>

namespace rbd {
namespace utils {

namespace detail {

template <typename T, void(T::*MF)(int)>
void aio_completion_callback(librbd::completion_t completion,
                                    void *arg) {
  librbd::RBD::AioCompletion *aio_completion =
    reinterpret_cast<librbd::RBD::AioCompletion*>(completion);

  // complete the AIO callback in separate thread context
  T *t = reinterpret_cast<T *>(arg);
  int r = aio_completion->get_return_value();
  aio_completion->release();

  (t->*MF)(r);
}

} // namespace detail

static const std::string RBD_DIFF_BANNER ("rbd diff v1\n");
static const size_t RBD_DEFAULT_SPARSE_SIZE = 4096;

static const std::string RBD_IMAGE_BANNER_V2 ("rbd image v2\n");
static const std::string RBD_IMAGE_DIFFS_BANNER_V2 ("rbd image diffs v2\n");
static const std::string RBD_DIFF_BANNER_V2 ("rbd diff v2\n");

#define RBD_DIFF_FROM_SNAP	'f'
#define RBD_DIFF_TO_SNAP	't'
#define RBD_DIFF_IMAGE_SIZE	's'
#define RBD_DIFF_WRITE		'w'
#define RBD_DIFF_ZERO		'z'
#define RBD_DIFF_END		'e'

#define RBD_SNAP_PROTECTION_STATUS     'p'

#define RBD_EXPORT_IMAGE_ORDER		'O'
#define RBD_EXPORT_IMAGE_FEATURES	'T'
#define RBD_EXPORT_IMAGE_STRIPE_UNIT	'U'
#define RBD_EXPORT_IMAGE_STRIPE_COUNT	'C'
#define RBD_EXPORT_IMAGE_META		'M'
#define RBD_EXPORT_IMAGE_END		'E'

enum SnapshotPresence {
  SNAPSHOT_PRESENCE_NONE,
  SNAPSHOT_PRESENCE_PERMITTED,
  SNAPSHOT_PRESENCE_REQUIRED
};

enum SpecValidation {
  SPEC_VALIDATION_FULL,
  SPEC_VALIDATION_SNAP,
  SPEC_VALIDATION_NONE
};

struct ProgressContext : public librbd::ProgressContext {
  const char *operation;
  bool progress;
  int last_pc;

  ProgressContext(const char *o, bool no_progress)
    : operation(o), progress(!no_progress), last_pc(0) {
  }

  int update_progress(uint64_t offset, uint64_t total) override;
  void finish();
  void fail();
};

template <typename T, void(T::*MF)(int)>
librbd::RBD::AioCompletion *create_aio_completion(T *t) {
  return new librbd::RBD::AioCompletion(
    t, &detail::aio_completion_callback<T, MF>);
}

void aio_context_callback(librbd::completion_t completion, void *arg);

int read_string(int fd, unsigned max, std::string *out);

int extract_spec(const std::string &spec, std::string *pool_name,
                 std::string *namespace_name, std::string *name,
                 std::string *snap_name, SpecValidation spec_validation);

std::string get_positional_argument(
    const boost::program_options::variables_map &vm, size_t index);

std::string get_default_pool_name();
int get_pool_and_namespace_names(
    const boost::program_options::variables_map &vm,
    bool default_empty_pool_name, bool validate_pool_name,
    std::string* pool_name, std::string* namespace_name, size_t *arg_index);

int get_pool_image_snapshot_names(
    const boost::program_options::variables_map &vm,
    argument_types::ArgumentModifier mod, size_t *spec_arg_index,
    std::string *pool_name, std::string *namespace_name,
    std::string *image_name, std::string *snap_name, bool image_name_required,
    SnapshotPresence snapshot_presence, SpecValidation spec_validation);

int get_pool_generic_snapshot_names(
    const boost::program_options::variables_map &vm,
    argument_types::ArgumentModifier mod, size_t *spec_arg_index,
    const std::string& pool_key, std::string *pool_name,
    std::string *namespace_name, const std::string& generic_key,
    const std::string& generic_key_desc, std::string *generic_name,
    std::string *snap_name, bool generic_name_required,
    SnapshotPresence snapshot_presence, SpecValidation spec_validation);

int get_pool_image_id(const boost::program_options::variables_map &vm,
                      size_t *spec_arg_index,
                      std::string *pool_name,
                      std::string *namespace_name,
                      std::string *image_id);

int validate_snapshot_name(argument_types::ArgumentModifier mod,
                           const std::string &snap_name,
                           SnapshotPresence snapshot_presence,
			   SpecValidation spec_validation);

int get_image_options(const boost::program_options::variables_map &vm,
                      bool get_format, librbd::ImageOptions* opts);

int get_journal_options(const boost::program_options::variables_map &vm,
			librbd::ImageOptions *opts);

int get_flatten_option(const boost::program_options::variables_map &vm,
                       librbd::ImageOptions *opts);

int get_image_size(const boost::program_options::variables_map &vm,
                   uint64_t *size);

int get_path(const boost::program_options::variables_map &vm,
             size_t *arg_index, std::string *path);

int get_formatter(const boost::program_options::variables_map &vm,
                  argument_types::Format::Formatter *formatter);

void init_context();

int init_rados(librados::Rados *rados);

int init(const std::string &pool_name, const std::string& namespace_name,
         librados::Rados *rados, librados::IoCtx *io_ctx);
int init_io_ctx(librados::Rados &rados, const std::string &pool_name,
                const std::string& namespace_name, librados::IoCtx *io_ctx);

void disable_cache();

int open_image(librados::IoCtx &io_ctx, const std::string &image_name,
               bool read_only, librbd::Image *image);

int open_image_by_id(librados::IoCtx &io_ctx, const std::string &image_id,
                     bool read_only, librbd::Image *image);

int init_and_open_image(const std::string &pool_name,
                        const std::string &namespace_name,
                        const std::string &image_name,
                        const std::string &image_id,
                        const std::string &snap_name, bool read_only,
                        librados::Rados *rados, librados::IoCtx *io_ctx,
                        librbd::Image *image);

int snap_set(librbd::Image &image, const std::string &snap_name);

void calc_sparse_extent(const bufferptr &bp,
                        size_t sparse_size,
			size_t buffer_offset,
                        uint64_t length,
                        size_t *write_length,
			bool *zeroed);

bool is_not_user_snap_namespace(librbd::Image* image,
                                const librbd::snap_info_t &snap_info);

std::string image_id(librbd::Image& image);

std::string mirror_image_state(librbd::mirror_image_state_t mirror_image_state);
std::string mirror_image_status_state(librbd::mirror_image_status_state_t state);
std::string mirror_image_status_state(librbd::mirror_image_status_t status);

std::string timestr(time_t t);

// duplicate here to not include librbd_internal lib
uint64_t get_rbd_default_features(CephContext* cct);

} // namespace utils
} // namespace rbd

#endif // CEPH_RBD_UTILS_H
