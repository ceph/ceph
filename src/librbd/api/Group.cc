// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Group.h"
#include "common/errno.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Group: " << __func__ << ": "

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;
// list binds to list() here, so std::list is explicitly used below

using ceph::bufferlist;
using librados::snap_t;
using librados::IoCtx;
using librados::Rados;

namespace librbd {
namespace api {

// Consistency groups functions

template <typename I>
int Group<I>::create(librados::IoCtx& io_ctx, const char *group_name)
{
  CephContext *cct = (CephContext *)io_ctx.cct();

  Rados rados(io_ctx);
  uint64_t bid = rados.get_instance_id();

  uint32_t extra = rand() % 0xFFFFFFFF;
  ostringstream bid_ss;
  bid_ss << std::hex << bid << std::hex << extra;
  string id = bid_ss.str();

  ldout(cct, 2) << "adding consistency group to directory..." << dendl;

  int r = cls_client::group_dir_add(&io_ctx, RBD_GROUP_DIRECTORY, group_name,
                                    id);
  if (r < 0) {
    lderr(cct) << "error adding consistency group to directory: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string header_oid = util::group_header_name(id);

  r = cls_client::group_create(&io_ctx, header_oid);
  if (r < 0) {
    lderr(cct) << "error writing header: " << cpp_strerror(r) << dendl;
    goto err_remove_from_dir;
  }

  return 0;

err_remove_from_dir:
  int remove_r = cls_client::group_dir_remove(&io_ctx, RBD_GROUP_DIRECTORY,
					      group_name, id);
  if (remove_r < 0) {
    lderr(cct) << "error cleaning up consistency group from rbd_directory "
	       << "object after creation failed: " << cpp_strerror(remove_r)
	       << dendl;
  }

  return r;
}

template <typename I>
int Group<I>::remove(librados::IoCtx& io_ctx, const char *group_name)
{
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "io_ctx=" << &io_ctx << " " << group_name << dendl;

  std::vector<group_image_status_t> images;
  int r = image_list(io_ctx, group_name, &images);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error listing group images" << dendl;
    return r;
  }

  for (auto i : images) {
    librados::Rados rados(io_ctx);
    IoCtx image_ioctx;
    rados.ioctx_create2(i.pool, image_ioctx);
    r = image_remove(io_ctx, group_name, image_ioctx, i.name.c_str());
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error removing image from a group" << dendl;
      return r;
    }
  }

  std::string group_id;
  r = cls_client::dir_get_id(&io_ctx, RBD_GROUP_DIRECTORY,
			     std::string(group_name), &group_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error getting id of group" << dendl;
    return r;
  }

  string header_oid = util::group_header_name(group_id);

  r = io_ctx.remove(header_oid);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing header: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::group_dir_remove(&io_ctx, RBD_GROUP_DIRECTORY, group_name,
                                   group_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing group from directory" << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Group<I>::list(IoCtx& io_ctx, vector<string> *names)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  int max_read = 1024;
  string last_read = "";
  int r;
  do {
    map<string, string> groups;
    r = cls_client::group_dir_list(&io_ctx, RBD_GROUP_DIRECTORY, last_read,
                                   max_read, &groups);
    if (r < 0) {
      lderr(cct) << "error listing group in directory: "
		 << cpp_strerror(r) << dendl;
      return r;
    }
    for (pair<string, string> group : groups) {
      names->push_back(group.first);
    }
    if (!groups.empty()) {
      last_read = groups.rbegin()->first;
    }
    r = groups.size();
  } while (r == max_read);

  return 0;
}

template <typename I>
int Group<I>::image_add(librados::IoCtx& group_ioctx, const char *group_name,
		    librados::IoCtx& image_ioctx, const char *image_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "io_ctx=" << &group_ioctx
		 << " group name " << group_name << " image "
		 << &image_ioctx << " name " << image_name << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY, group_name,
                                 &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);


  ldout(cct, 20) << "adding image to group name " << group_name
		 << " group id " << group_header_oid << dendl;

  string image_id;

  r = cls_client::dir_get_id(&image_ioctx, RBD_DIRECTORY, image_name,
                             &image_id);
  if (r < 0) {
    lderr(cct) << "error reading image id object: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  string image_header_oid = util::header_name(image_id);

  ldout(cct, 20) << "adding image " << image_name
		 << " image id " << image_header_oid << dendl;

  cls::rbd::GroupImageStatus incomplete_st(
    image_id, image_ioctx.get_id(),
    cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);
  cls::rbd::GroupImageStatus attached_st(
    image_id, image_ioctx.get_id(), cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED);

  r = cls_client::group_image_set(&group_ioctx, group_header_oid,
				  incomplete_st);

  cls::rbd::GroupSpec group_spec(group_id, group_ioctx.get_id());

  if (r < 0) {
    lderr(cct) << "error adding image reference to consistency group: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::image_add_group(&image_ioctx, image_header_oid, group_spec);
  if (r < 0) {
    lderr(cct) << "error adding group reference to image: "
	       << cpp_strerror(-r) << dendl;
    cls::rbd::GroupImageSpec spec(image_id, image_ioctx.get_id());
    cls_client::group_image_remove(&group_ioctx, group_header_oid, spec);
    // Ignore errors in the clean up procedure.
    return r;
  }

  r = cls_client::group_image_set(&group_ioctx, group_header_oid,
				  attached_st);

  return r;
}

template <typename I>
int Group<I>::image_remove(librados::IoCtx& group_ioctx, const char *group_name,
		           librados::IoCtx& image_ioctx, const char *image_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "io_ctx=" << &group_ioctx
		 << " group name " << group_name << " image "
		 << &image_ioctx << " name " << image_name << dendl;

  string image_id;
  int r = cls_client::dir_get_id(&image_ioctx, RBD_DIRECTORY, image_name,
                                 &image_id);
  if (r < 0) {
    lderr(cct) << "error reading image id object: "
               << cpp_strerror(-r) << dendl;
    return r;
  }

  return Group<I>::image_remove_by_id(group_ioctx, group_name, image_ioctx,
                                      image_id.c_str());
}

template <typename I>
int Group<I>::image_remove_by_id(librados::IoCtx& group_ioctx,
                                 const char *group_name,
                                 librados::IoCtx& image_ioctx,
                                 const char *image_id)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "group_remove_image_by_id " << &group_ioctx
                 << " group name " << group_name << " image "
                 << &image_ioctx << " id " << image_id << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY, group_name,
                                 &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  ldout(cct, 20) << "adding image to group name " << group_name
		 << " group id " << group_header_oid << dendl;

  string image_header_oid = util::header_name(image_id);

  ldout(cct, 20) << "removing " << " image id " << image_header_oid << dendl;

  cls::rbd::GroupSpec group_spec(group_id, group_ioctx.get_id());

  cls::rbd::GroupImageStatus incomplete_st(
    image_id, image_ioctx.get_id(),
    cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);

  cls::rbd::GroupImageSpec spec(image_id, image_ioctx.get_id());

  r = cls_client::group_image_set(&group_ioctx, group_header_oid,
				  incomplete_st);

  if (r < 0) {
    lderr(cct) << "couldn't put image into removing state: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::image_remove_group(&image_ioctx, image_header_oid,
				     group_spec);
  if ((r < 0) && (r != -ENOENT)) {
    lderr(cct) << "couldn't remove group reference from image"
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::group_image_remove(&group_ioctx, group_header_oid, spec);
  if (r < 0) {
    lderr(cct) << "couldn't remove image from group"
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Group<I>::image_list(librados::IoCtx& group_ioctx,
		     const char *group_name,
		     std::vector<group_image_status_t> *images)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "io_ctx=" << &group_ioctx
		 << " group name " << group_name << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
				 group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  ldout(cct, 20) << "listing images in group name "
		 << group_name << " group id " << group_header_oid << dendl;

  std::vector<cls::rbd::GroupImageStatus> image_ids;

  const int max_read = 1024;
  do {
    std::vector<cls::rbd::GroupImageStatus> image_ids_page;
    cls::rbd::GroupImageSpec start_last;

    r = cls_client::group_image_list(&group_ioctx, group_header_oid,
                                     start_last, max_read, &image_ids_page);

    if (r < 0) {
      lderr(cct) << "error reading image list from consistency group: "
	<< cpp_strerror(-r) << dendl;
      return r;
    }
    image_ids.insert(image_ids.end(),
		     image_ids_page.begin(), image_ids_page.end());

    if (image_ids_page.size() > 0)
      start_last = image_ids_page.rbegin()->spec;

    r = image_ids_page.size();
  } while (r == max_read);

  for (auto i : image_ids) {
    librados::Rados rados(group_ioctx);
    IoCtx ioctx;
    rados.ioctx_create2(i.spec.pool_id, ioctx);
    std::string image_name;
    r = cls_client::dir_get_name(&ioctx, RBD_DIRECTORY,
				 i.spec.image_id, &image_name);
    if (r < 0) {
      return r;
    }

    images->push_back(
	group_image_status_t {
	   image_name,
	   i.spec.pool_id,
	   static_cast<group_image_state_t>(i.state)});
  }

  return 0;
}

template <typename I>
int Group<I>::image_get_group(I *ictx, group_spec_t *group_spec)
{
  int r = ictx->state->refresh_if_required();
  if (r < 0)
    return r;

  if (-1 != ictx->group_spec.pool_id) {
    librados::Rados rados(ictx->md_ctx);
    IoCtx ioctx;
    rados.ioctx_create2(ictx->group_spec.pool_id, ioctx);

    std::string group_name;
    r = cls_client::dir_get_name(&ioctx, RBD_GROUP_DIRECTORY,
				 ictx->group_spec.group_id, &group_name);
    if (r < 0)
      return r;
    group_spec->pool = ictx->group_spec.pool_id;
    group_spec->name = group_name;
  } else {
    group_spec->pool = -1;
    group_spec->name = "";
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Group<librbd::ImageCtx>;
