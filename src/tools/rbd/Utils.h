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

static const std::string RBD_DIFF_BANNER ("rbd diff v1\n");

enum SnapshotPresence {
  SNAPSHOT_PRESENCE_NONE,
  SNAPSHOT_PRESENCE_PERMITTED,
  SNAPSHOT_PRESENCE_REQUIRED
};

struct ProgressContext : public librbd::ProgressContext {
  const char *operation;
  bool progress;
  int last_pc;

  ProgressContext(const char *o, bool no_progress)
    : operation(o), progress(!no_progress), last_pc(0) {
  }

  int update_progress(uint64_t offset, uint64_t total);
  void finish();
  void fail();
};

void aio_context_callback(librbd::completion_t completion, void *arg);

int read_string(int fd, unsigned max, std::string *out);

int extract_spec(const std::string &spec, std::string *pool_name,
                 std::string *image_name, std::string *snap_name);

std::string get_positional_argument(
    const boost::program_options::variables_map &vm, size_t index);

std::string get_pool_name(const boost::program_options::variables_map &vm,
                          size_t *arg_index);

int get_pool_image_snapshot_names(
    const boost::program_options::variables_map &vm,
    argument_types::ArgumentModifier mod, size_t *spec_arg_index,
    std::string *pool_name, std::string *image_name, std::string *snap_name,
    SnapshotPresence snapshot_presence, bool image_required = true);

int get_pool_journal_names(
    const boost::program_options::variables_map &vm,
    argument_types::ArgumentModifier mod, size_t *spec_arg_index,
    std::string *pool_name, std::string *journal_name);

int validate_snapshot_name(argument_types::ArgumentModifier mod,
                           const std::string &snap_name,
                           SnapshotPresence snapshot_presence);

int get_image_options(const boost::program_options::variables_map &vm,
                      bool get_format, librbd::ImageOptions* opts);

int get_journal_options(const boost::program_options::variables_map &vm,
			librbd::ImageOptions *opts);

int get_image_size(const boost::program_options::variables_map &vm,
                   uint64_t *size);

int get_path(const boost::program_options::variables_map &vm,
             const std::string &positional_path, std::string *path);

int get_formatter(const boost::program_options::variables_map &vm,
                  argument_types::Format::Formatter *formatter);

void init_context();

int init(const std::string &pool_name, librados::Rados *rados,
         librados::IoCtx *io_ctx);

int init_io_ctx(librados::Rados &rados, const std::string &pool_name,
                librados::IoCtx *io_ctx);

int open_image(librados::IoCtx &io_ctx, const std::string &image_name,
               bool read_only, librbd::Image *image);

int init_and_open_image(const std::string &pool_name,
                        const std::string &image_name,
                        const std::string &snap_name, bool read_only,
                        librados::Rados *rados, librados::IoCtx *io_ctx,
                        librbd::Image *image);

int snap_set(librbd::Image &image, const std::string snap_name);

std::string image_id(librbd::Image& image);

} // namespace utils
} // namespace rbd

#endif // CEPH_RBD_UTILS_H
