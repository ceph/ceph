// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/compat.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "include/encoding.h"
#include <iostream>
#include <fcntl.h>
#include <stdlib.h>
#include <boost/program_options.hpp>
#include <boost/scope_exit.hpp>

using std::cerr;
using std::string;

namespace rbd {
namespace action {
namespace export_full {

struct ExportDiffContext {
  librbd::Image *image;
  int fd;
  int export_format;
  uint64_t totalsize;
  utils::ProgressContext pc;
  OrderedThrottle throttle;

  ExportDiffContext(librbd::Image *i, int f, uint64_t t, int max_ops,
                    bool no_progress, int eformat) :
    image(i), fd(f), export_format(eformat), totalsize(t), pc("Exporting image", no_progress),
    throttle(max_ops, true) {
  }
};

class C_ExportDiff : public Context {
public:
  C_ExportDiff(ExportDiffContext *edc, uint64_t offset, uint64_t length,
               bool exists, int export_format)
    : m_export_diff_context(edc), m_offset(offset), m_length(length),
      m_exists(exists), m_export_format(export_format) {
  }

  int send() {
    if (m_export_diff_context->throttle.pending_error()) {
      return m_export_diff_context->throttle.wait_for_ret();
    }

    C_OrderedThrottle *ctx = m_export_diff_context->throttle.start_op(this);
    if (m_exists) {
      librbd::RBD::AioCompletion *aio_completion =
        new librbd::RBD::AioCompletion(ctx, &utils::aio_context_callback);

      int op_flags = LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
      int r = m_export_diff_context->image->aio_read2(
        m_offset, m_length, m_read_data, aio_completion, op_flags);
      if (r < 0) {
        aio_completion->release();
        ctx->complete(r);
      }
    } else {
      ctx->complete(0);
    }
    return 0;
  }

  static int export_diff_cb(uint64_t offset, size_t length, int exists,
			    void *arg) {
    ExportDiffContext *edc = reinterpret_cast<ExportDiffContext *>(arg);

    C_ExportDiff *context = new C_ExportDiff(edc, offset, length, exists, edc->export_format);
    return context->send();
  }

protected:
  void finish(int r) override {
    if (r >= 0) {
      if (m_exists) {
        m_exists = !m_read_data.is_zero();
      }
      r = write_extent(m_export_diff_context, m_offset, m_length, m_exists, m_export_format);
      if (r == 0 && m_exists) {
        r = m_read_data.write_fd(m_export_diff_context->fd);
      }
    }
    m_export_diff_context->throttle.end_op(r);
  }

private:
  ExportDiffContext *m_export_diff_context;
  uint64_t m_offset;
  uint64_t m_length;
  bool m_exists;
  int m_export_format;
  bufferlist m_read_data;

  static int write_extent(ExportDiffContext *edc, uint64_t offset,
                          uint64_t length, bool exists, int export_format) {
    // extent
    bufferlist bl;
    __u8 tag = exists ? RBD_DIFF_WRITE : RBD_DIFF_ZERO;
    uint64_t len = 0;
    encode(tag, bl);
    if (export_format == 2) {
      if (tag == RBD_DIFF_WRITE)
	len = 8 + 8 + length;
      else
	len = 8 + 8;
      encode(len, bl);
    }
    encode(offset, bl);
    encode(length, bl);
    int r = bl.write_fd(edc->fd);

    edc->pc.update_progress(offset, edc->totalsize);
    return r;
  }
};

int encode_trash_namespace(
  librbd::Image &image,
  uint64_t snap_id,
  int export_format,
  bufferlist &bl)
{
  librbd::snap_trash_namespace_t trash_namespace;
  int r = image.snap_get_trash_namespace2(
    snap_id, &trash_namespace, sizeof(trash_namespace));
  if (r < 0) {
    return r;
  }
  if (trash_namespace.original_namespace_type ==
        librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_GROUP ||
      trash_namespace.original_namespace_type ==
        librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_MIRROR) {
    std::cerr << "snap_id corresponds to a trashed snapshot that"
              << "corresponds to the group namespace"
              << ", which export-diff doesn't allow"
              << std::endl;
    return -EINVAL;
  }
  // although trash_namespace.original_namespace_type must be USER for now,
  // we still encode it, in case of any future namespace type expansion.
  encode(trash_namespace.original_namespace_type, bl);
  if (export_format == 2) {
    uint64_t len = trash_namespace.original_name.length() + 4;
    encode(len, bl);
  }
  encode(trash_namespace.original_name, bl);
  return r;
}

int do_export_diff_fd(librbd::Image& image, const char *fromsnapname,
                      const char *endsnapname, uint64_t from_snap_id,
                      uint64_t snap_id,
                      librbd::snap_namespace_type_t from_snap_ns_type,
                      librbd::snap_namespace_type_t snap_ns_type,
                      bool whole_object, int fd,
                      bool no_progress, int export_format)
{
  int r;
  librbd::image_info_t info;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  {
    // header
    bufferlist bl;
    if (export_format == 1)
      bl.append(utils::RBD_DIFF_BANNER);
    else
      bl.append(utils::RBD_DIFF_BANNER_V2);

    __u8 tag;
    uint64_t len = 0;
    if (fromsnapname) {
      tag = RBD_DIFF_FROM_SNAP;
      encode(tag, bl);
      encode(from_snap_ns_type, bl);
      std::string from(fromsnapname);
      if (export_format == 2) {
	len = from.length() + 4;
	encode(len, bl);
      }
      encode(from, bl);
      if (from_snap_ns_type ==
            librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_TRASH) {
        encode_trash_namespace(image, from_snap_id, export_format, bl);
      }
    }

    if (endsnapname) {
      tag = RBD_DIFF_TO_SNAP;
      encode(tag, bl);
      encode(snap_ns_type, bl);
      std::string to(endsnapname);
      if (export_format == 2) {
        len = to.length() + 4;
        encode(len, bl);
      }
      encode(to, bl);
      if (snap_ns_type ==
            librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_TRASH) {
        encode_trash_namespace(image, snap_id, export_format, bl);
      }
    }

    if (snap_ns_type ==
          librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_USER &&
        endsnapname &&
        export_format == 2) {
      tag = RBD_SNAP_PROTECTION_STATUS;
      encode(tag, bl);
      bool is_protected = false;
      r = image.snap_is_protected(endsnapname, &is_protected);
      if (r < 0) {
        return r;
      }
      len = 1;
      encode(len, bl);
      encode(is_protected, bl);
    }

    tag = RBD_DIFF_IMAGE_SIZE;
    encode(tag, bl);
    uint64_t endsize = info.size;
    if (export_format == 2) {
      len = 8;
      encode(len, bl);
    }
    encode(endsize, bl);

    r = bl.write_fd(fd);
    if (r < 0) {
      return r;
    }
  }
  ExportDiffContext edc(&image, fd, info.size,
                        g_conf().get_val<uint64_t>("rbd_concurrent_management_ops"),
                        no_progress, export_format);
  uint32_t flags = RBD_DIFF_ITERATE_FLAG_INCLUDE_PARENT;
  if (whole_object) {
    flags |= RBD_DIFF_ITERATE_FLAG_WHOLE_OBJECT;
  }
  if (from_snap_id > CEPH_MAXSNAP) {
    from_snap_id = 0;
  }
  r = image.diff_iterate3(from_snap_id, 0, info.size, flags,
                          &C_ExportDiff::export_diff_cb, (void *)&edc);
  if (r < 0) {
    goto out;
  }

  r = edc.throttle.wait_for_ret();
  if (r < 0) {
    goto out;
  }

  {
    __u8 tag = RBD_DIFF_END;
    bufferlist bl;
    encode(tag, bl);
    r = bl.write_fd(fd);
  }

out:
  if (r < 0)
    edc.pc.fail();
  else
    edc.pc.finish();

  return r;
}

int do_export_diff(librbd::Image& image, const char *fromsnapname,
                   const char *endsnapname, uint64_t from_snap_id,
                   uint64_t snap_id,
                   librbd::snap_namespace_type_t from_snap_ns_type,
                   librbd::snap_namespace_type_t snap_ns_type,
                   bool whole_object, const char *path, bool no_progress)
{
  int r;
  int fd;

  if (strcmp(path, "-") == 0)
    fd = STDOUT_FILENO;
  else
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_BINARY, 0644);
  if (fd < 0)
    return -errno;

  r = do_export_diff_fd(image, fromsnapname, endsnapname, from_snap_id,
                        snap_id, from_snap_ns_type, snap_ns_type,
                        whole_object, fd, no_progress, 1);

  if (fd != 1)
    close(fd);
  if (r < 0 && fd != 1) {
    remove(path);
  }

  return r;
}


namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments_diff(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_SOURCE);
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  at::add_snap_id_option(options, at::ARGUMENT_MODIFIER_DEST);
  options->add_options()
    (at::FROM_SNAPSHOT_NAME.c_str(), po::value<std::string>(),
     "snapshot starting point")
    (at::FROM_SNAPSHOT_ID.c_str(), po::value<uint64_t>(),
     "snapshot starting id")
    (at::WHOLE_OBJECT.c_str(), po::bool_switch(), "compare whole object");
  at::add_no_progress_option(options);
}

int execute_diff(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  std::string from_snap_name;
  if (vm.count(at::FROM_SNAPSHOT_NAME)) {
    from_snap_name = vm[at::FROM_SNAPSHOT_NAME].as<std::string>();
  }

  uint64_t from_snap_id = CEPH_NOSNAP;
  if (vm.count(at::FROM_SNAPSHOT_ID)) {
    if (!from_snap_name.empty()) {
      std::cerr << "--from-snap and --from-snap-id can't be set at the same time"
        << std::endl;
      return -EINVAL;
    }
    from_snap_id = vm[at::FROM_SNAPSHOT_ID].as<uint64_t>();
    if (from_snap_id >= CEPH_MAXSNAP) {
      std::cerr << "invalid --from-snap-id" << std::endl;
      return -EINVAL;
    }
  }

  uint64_t snap_id = CEPH_NOSNAP;
  if (vm.count(at::SNAPSHOT_ID)) {
    if (!snap_name.empty()) {
      std::cerr << "--snap and --snap-id can't be set at the same time"
        << std::endl;
      return -EINVAL;
    }
    snap_id = vm[at::SNAPSHOT_ID].as<uint64_t>();
    if (snap_id >= CEPH_MAXSNAP) {
      std::cerr << "invalid --snap-id" << std::endl;
      return -EINVAL;
    }
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name,
                                 image_name, "",
                                 snap_name, true, &rados,
                                 &io_ctx, &image, snap_id);
  if (r < 0) {
    return r;
  }

  if (!from_snap_name.empty()) {
    ceph_assert(from_snap_id == 0);
    r = image.snap_get_id(from_snap_name, &from_snap_id);
    if (r < 0) {
      return r;
    }
  }

  if (!snap_name.empty()) {
    ceph_assert(snap_id == CEPH_NOSNAP);
    r = image.snap_get_id(snap_name, &snap_id);
    if (r < 0) {
      return r;
    }
  }

  librbd::snap_namespace_type_t from_snap_ns_type =
    librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_USER;
  if (from_snap_id != CEPH_NOSNAP) {
    r = image.snap_get_name(from_snap_id, &from_snap_name);
    if (r < 0) {
      return r;
    }
    r = image.snap_get_namespace_type(from_snap_id, &from_snap_ns_type);
    if (r < 0) {
      return r;
    }
    if (from_snap_ns_type ==
          librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_MIRROR) {
      std::cerr << "from_snap_id corresponds to a snapshot in the mirror namespace"
                << ", which export-diff doesn't allow"
                << std::endl;
      return -EINVAL;
    }
    if (from_snap_ns_type ==
          librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_GROUP) {
      std::cerr << "from_snap_id corresponds to a snapshot in the group namespace"
                << ", which export-diff doesn't allow"
                << std::endl;
      return -EINVAL;
    }
  }

  librbd::snap_namespace_type_t snap_ns_type =
    librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_USER;
  if (snap_id != CEPH_NOSNAP) {
    r = image.snap_get_name(snap_id, &snap_name);
    if (r < 0) {
      return r;
    }
    r = image.snap_get_namespace_type(snap_id, &snap_ns_type);
    if (r < 0) {
      return r;
    }
    if (snap_ns_type ==
          librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_MIRROR) {
      std::cerr << "snap_id corresponds to a snapshot in the mirror namespace"
                << ", which export-diff doesn't allow"
                << std::endl;
      return -EINVAL;
    }
    if (snap_ns_type ==
          librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_GROUP) {
      std::cerr << "snap_id corresponds to a snapshot in the group namespace"
                << ", which export-diff doesn't allow"
                << std::endl;
      return -EINVAL;
    }
  }

  r = do_export_diff(image,
                     from_snap_name.empty() ? nullptr : from_snap_name.c_str(),
                     snap_name.empty() ? nullptr : snap_name.c_str(),
                     from_snap_id, snap_id,
                     from_snap_ns_type, snap_ns_type,
                     vm[at::WHOLE_OBJECT].as<bool>(), path.c_str(),
                     vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: export-diff error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_diff(
  {"export-diff"}, {}, "Export incremental diff to file.", "",
  &get_arguments_diff, &execute_diff);

class C_Export : public Context
{
public:
  C_Export(OrderedThrottle &ordered_throttle, librbd::Image &image,
	   uint64_t fd_offset, uint64_t offset, uint64_t length, int fd)
    : m_throttle(ordered_throttle), m_image(image), m_dest_offset(fd_offset),
      m_offset(offset), m_length(length), m_fd(fd)
  {
  }

  void send()
  {
    auto ctx = m_throttle.start_op(this);
    auto aio_completion = new librbd::RBD::AioCompletion(
      ctx, &utils::aio_context_callback);
    int op_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                   LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
    int r = m_image.aio_read2(m_offset, m_length, m_bufferlist,
                              aio_completion, op_flags);
    if (r < 0) {
      cerr << "rbd: error requesting read from source image" << std::endl;
      aio_completion->release();
      m_throttle.end_op(r);
    }
  }

  void finish(int r) override
  {
    BOOST_SCOPE_EXIT((&m_throttle) (&r))
    {
      m_throttle.end_op(r);
    } BOOST_SCOPE_EXIT_END

    if (r < 0) {
      cerr << "rbd: error reading from source image at offset "
           << m_offset << ": " << cpp_strerror(r) << std::endl;
      return;
    }

    ceph_assert(m_bufferlist.length() == static_cast<size_t>(r));
    if (m_fd != STDOUT_FILENO) {
      if (m_bufferlist.is_zero()) {
        return;
      }

      uint64_t chkret = lseek64(m_fd, m_dest_offset, SEEK_SET);
      if (chkret != m_dest_offset) {
        cerr << "rbd: error seeking destination image to offset "
             << m_dest_offset << std::endl;
        r = -errno;
        return;
      }
    }

    r = m_bufferlist.write_fd(m_fd);
    if (r < 0) {
      cerr << "rbd: error writing to destination image at offset "
           << m_dest_offset << std::endl;
    }
  }

private:
  OrderedThrottle &m_throttle;
  librbd::Image &m_image;
  bufferlist m_bufferlist;
  uint64_t m_dest_offset;
  uint64_t m_offset;
  uint64_t m_length;
  int m_fd;
};

const uint32_t MAX_KEYS = 64;

static int do_export_v2(librbd::Image& image, librbd::image_info_t &info, int fd,
		        uint64_t period, int max_concurrent_ops, utils::ProgressContext &pc)
{
  int r = 0;
  // header
  bufferlist bl;
  bl.append(utils::RBD_IMAGE_BANNER_V2);

  __u8 tag;
  uint64_t length;
  // encode order
  tag = RBD_EXPORT_IMAGE_ORDER;
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(uint64_t(info.order), bl);

  // encode features
  tag = RBD_EXPORT_IMAGE_FEATURES;
  uint64_t features;
  image.features(&features);
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(features, bl);

  // encode stripe_unit and stripe_count
  tag = RBD_EXPORT_IMAGE_STRIPE_UNIT;
  uint64_t stripe_unit;
  stripe_unit = image.get_stripe_unit();
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(stripe_unit, bl);

  tag = RBD_EXPORT_IMAGE_STRIPE_COUNT;
  uint64_t stripe_count;
  stripe_count = image.get_stripe_count();
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(stripe_count, bl);

  //retrieve metadata of image
  std::map<std::string, string> imagemetas;
  std::string last_key;
  bool more_results = true;
  while (more_results) {
    std::map<std::string, bufferlist> pairs;
    r = image.metadata_list(last_key, MAX_KEYS, &pairs);
    if (r < 0) {
      std::cerr << "failed to retrieve metadata of image : " << cpp_strerror(r)
                << std::endl;
      return r;
    }

    if (!pairs.empty()) {
      last_key = pairs.rbegin()->first;

      for (auto kv : pairs) {
        std::string key = kv.first;
        std::string val(kv.second.c_str(), kv.second.length());
        imagemetas[key] = val;
      }
    }
    more_results = (pairs.size() == MAX_KEYS);
  }

  //encode imageMeta key and value
  for (std::map<std::string, string>::iterator it = imagemetas.begin();
       it != imagemetas.end(); ++it) {
    string key = it->first;
    string value = it->second;

    tag = RBD_EXPORT_IMAGE_META;
    length = key.length() + value.length() + 4 * 2;
    encode(tag, bl);
    encode(length, bl);
    encode(key, bl);
    encode(value, bl);
  }

  // encode end tag
  tag = RBD_EXPORT_IMAGE_END;
  encode(tag, bl);

  // write bl to fd.
  r = bl.write_fd(fd);
  if (r < 0) {
    return r;
  }

  // header for snapshots
  bl.clear();
  bl.append(utils::RBD_IMAGE_DIFFS_BANNER_V2);

  std::vector<librbd::snap_info_t> snaps;
  r = image.snap_list(snaps);
  if (r < 0) {
    return r;
  }

  uint64_t diff_num = snaps.size() + 1;
  encode(diff_num, bl);

  r = bl.write_fd(fd);
  if (r < 0) {
    return r;
  }

  const char *last_snap = NULL;
  uint64_t last_snap_id = CEPH_NOSNAP;
  librbd::snap_namespace_type_t last_snap_ns_type =
    librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_USER;
  for (size_t i = 0; i < snaps.size(); ++i) {
    utils::snap_set(image, snaps[i].name.c_str());
    utils::snap_set(image, snaps[i].id);
    librbd::snap_namespace_type_t snap_ns_type =
      librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_USER;
    r = image.snap_get_namespace_type(snaps[i].id, &snap_ns_type);
    if (r < 0) {
      return r;
    }

    r = do_export_diff_fd(image, last_snap, snaps[i].name.c_str(),
                          last_snap_id, snaps[i].id,
                          last_snap_ns_type, snap_ns_type,
                          false, fd, true, 2);
    last_snap_ns_type = snap_ns_type;
    if (r < 0) {
      return r;
    }
    pc.update_progress(i, snaps.size() + 1);
    last_snap = snaps[i].name.c_str();
    last_snap_id = snaps[i].id;
  }
  utils::snap_set(image, std::string(""));
  utils::snap_set(image, CEPH_NOSNAP);
  r = do_export_diff_fd(image, last_snap, nullptr, last_snap_id,
                        CEPH_NOSNAP, last_snap_ns_type,
                        librbd::snap_namespace_type_t::RBD_SNAP_NAMESPACE_TYPE_USER,
                        false, fd, true, 2);
  if (r < 0) {
    return r;
  }
  pc.update_progress(snaps.size() + 1, snaps.size() + 1);
  return r;
}

static int do_export_v1(librbd::Image& image, librbd::image_info_t &info,
                        int fd, uint64_t period, int max_concurrent_ops,
                        utils::ProgressContext &pc)
{
  int r = 0;
  size_t file_size = 0;
  OrderedThrottle throttle(max_concurrent_ops, false);
  for (uint64_t offset = 0; offset < info.size; offset += period) {
    if (throttle.pending_error()) {
      break;
    }

    uint64_t length = std::min(period, info.size - offset);
    C_Export *ctx = new C_Export(throttle, image, file_size + offset, offset,
                                 length, fd);
    ctx->send();

    pc.update_progress(offset, info.size);
  }

  file_size += info.size;
  r = throttle.wait_for_ret();
  if (fd != 1) {
    if (r >= 0) {
      r = ftruncate(fd, file_size);
      if (r < 0)
	return r;

      uint64_t chkret = lseek64(fd, file_size, SEEK_SET);
      if (chkret != file_size)
	r = errno;
    }
  }
  return r;
}

static int do_export(librbd::Image& image, const char *path, bool no_progress,
                     int export_format)
{
  librbd::image_info_t info;
  int64_t r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  int fd;
  int max_concurrent_ops = g_conf().get_val<uint64_t>("rbd_concurrent_management_ops");
  bool to_stdout = (strcmp(path, "-") == 0);
  if (to_stdout) {
    fd = STDOUT_FILENO;
  } else {
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_BINARY, 0644);
    if (fd < 0) {
      return -errno;
    }
#ifdef HAVE_POSIX_FADVISE
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
  }

  utils::ProgressContext pc("Exporting image", no_progress);
  uint64_t period = image.get_stripe_count() * (1ull << info.order);

  if (export_format == 1)
    r = do_export_v1(image, info, fd, period, max_concurrent_ops, pc);
  else
    r = do_export_v2(image, info, fd, period, max_concurrent_ops, pc);

  if (r < 0)
    pc.fail();
  else
    pc.finish();
  if (!to_stdout)
    close(fd);
  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_SOURCE);
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  at::add_no_progress_option(options);
  at::add_export_format_option(options);
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "",
                                 snap_name, true, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  int format = 1;
  if (vm.count("export-format"))
    format = vm["export-format"].as<uint64_t>();

  r = do_export(image, path.c_str(), vm[at::NO_PROGRESS].as<bool>(), format);
  if (r < 0) {
    std::cerr << "rbd: export error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"export"}, {}, "Export image to file.", "", &get_arguments, &execute);

} // namespace export_full
} // namespace action
} // namespace rbd
