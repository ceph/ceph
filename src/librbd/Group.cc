// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"

#include "librbd/AioCompletion.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Group.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Group: "

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

// Auxiliary functions

static string generate_uuid(librados::IoCtx& io_ctx)
{
  Rados rados(io_ctx);
  uint64_t bid = rados.get_instance_id();

  uint32_t extra = rand() % 0xFFFFFFFF;
  ostringstream bid_ss;
  bid_ss << std::hex << bid << std::hex << extra;
  return bid_ss.str();
}

static int group_snap_info(librados::IoCtx& group_ioctx,
			   const char *group_name,
			   const char *snap_name,
			   cls::rbd::GroupSnapshot *cls_snap)
{
  if (nullptr == cls_snap) {
    return -EINVAL;
  }
  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);

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

  const int max_read = 1024;
  cls::rbd::GroupSnapshot snap_last;

  for (;;) {
    vector<cls::rbd::GroupSnapshot> snaps_page;

    r = cls_client::group_snap_list(&group_ioctx, group_header_oid,
				    snap_last, max_read, &snaps_page);

    if (r < 0) {
      lderr(cct) << "error reading snap list from consistency group: " <<
		    cpp_strerror(-r) << dendl;
      return r;
    }
    for (cls::rbd::GroupSnapshot &snap : snaps_page) {
      if (snap.name == string(snap_name)) {
	*cls_snap = snap;
	goto exit;
      }
    }
    if (snaps_page.size() < max_read) {
      break;
    }
    snap_last = *snaps_page.rbegin();
  }

exit:
  return 0;
}

static int group_snap_list(librados::IoCtx& group_ioctx, const char *group_name,
			   std::vector<cls::rbd::GroupSnapshot> *cls_snaps)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);

  string group_id;
  vector<string> ind_snap_names;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
				 group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  const int max_read = 1024;
  cls::rbd::GroupSnapshot snap_last;

  for (;;) {
    vector<cls::rbd::GroupSnapshot> snaps_page;

    r = cls_client::group_snap_list(&group_ioctx, group_header_oid,
				    snap_last, max_read, &snaps_page);

    if (r < 0) {
      lderr(cct) << "error reading snap list from consistency group: "
	<< cpp_strerror(-r) << dendl;
      return r;
    }
    cls_snaps->insert(cls_snaps->end(), snaps_page.begin(), snaps_page.end());
    if (snaps_page.size() < max_read) {
      break;
    }
    snap_last = *snaps_page.rbegin();
  }

  return 0;
}

static std::string calc_ind_image_snap_name(uint64_t pool_id,
					    std::string group_id,
					    std::string snap_id)
{
  std::stringstream ind_snap_name_stream;
  ind_snap_name_stream << std::setw(16) << std::setfill('0') << std::hex <<
			  pool_id <<
			  "_" << group_id <<
			  "_" << snap_id;
  return ind_snap_name_stream.str();
}

// Consistency groups functions

int group_create(librados::IoCtx& io_ctx, const char *group_name)
{
  CephContext *cct = (CephContext *)io_ctx.cct();

  string id = generate_uuid(io_ctx);

  ldout(cct, 2) << "adding consistency group to directory..." << dendl;

  int r = cls_client::group_dir_add(&io_ctx, RBD_GROUP_DIRECTORY, group_name, id);
  if (r < 0) {
    lderr(cct) << "error adding consistency group to directory: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string header_oid = util::group_header_name(id);

  r = io_ctx.create(header_oid, true);
  if (r < 0) {
    lderr(cct) << "error creating group header: " << cpp_strerror(r) << dendl;
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

static int group_image_list(librados::IoCtx& group_ioctx, const char *group_name,
			    std::vector<cls::rbd::GroupImageStatus> *image_ids)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();

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
  image_ids->clear();

  const int max_read = 1024;
  cls::rbd::GroupImageSpec start_last;
  do {
    std::vector<cls::rbd::GroupImageStatus> image_ids_page;

    r = cls_client::group_image_list(&group_ioctx, group_header_oid,
				     start_last, max_read, &image_ids_page);

    if (r < 0) {
      lderr(cct) << "error reading image list from consistency group: "
	<< cpp_strerror(-r) << dendl;
      return r;
    }
    image_ids->insert(image_ids->end(),
		     image_ids_page.begin(), image_ids_page.end());

    if (image_ids_page.size() > 0)
      start_last = image_ids_page.rbegin()->spec;

    r = image_ids_page.size();
  } while (r == max_read);

  return 0;
}

static int group_image_remove(librados::IoCtx& group_ioctx, string group_id,
			      librados::IoCtx& image_ioctx, string image_id)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();

  string group_header_oid = util::group_header_name(group_id);

  string image_header_oid = util::header_name(image_id);

  ldout(cct, 20) << "removing image " << image_id
		 << " image id " << image_header_oid << dendl;

  cls::rbd::GroupSpec group_spec(group_id, group_ioctx.get_id());

  cls::rbd::GroupImageStatus incomplete_st(image_id, image_ioctx.get_id(),
				cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);

  cls::rbd::GroupImageSpec spec(image_id, image_ioctx.get_id());

  int r = cls_client::group_image_set(&group_ioctx, group_header_oid,
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

static int group_snap_remove_by_record(librados::IoCtx& group_ioctx,
				       const cls::rbd::GroupSnapshot& group_snap,
				       const std::string& group_id,
				       const std::string& group_header_oid) {

  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);
  std::vector<C_SaferCond*> on_finishes;
  int r, ret_code;

  std::vector<librbd::IoCtx*> io_ctxs;
  std::vector<librbd::ImageCtx*> ictxs;

  std::string image_snap_name;

  cls::rbd::SnapshotNamespace ne;

  ldout(cct, 20) << "Removing snapshots" << dendl;
  int snap_count = group_snap.snaps.size();

  for (int i = 0; i < snap_count; ++i) {
    librbd::IoCtx* image_io_ctx = new librbd::IoCtx;
    r = rados.ioctx_create2(group_snap.snaps[i].pool, *image_io_ctx);
    if (r < 0) {
      ldout(cct, 1) << "Failed to create io context for image" << dendl;
    }

    librbd::ImageCtx* image_ctx = new ImageCtx("", group_snap.snaps[i].image_id,
					       nullptr, *image_io_ctx, false);

    C_SaferCond* on_finish = new C_SaferCond;

    image_ctx->state->open(on_finish);

    ictxs.push_back(image_ctx);
    on_finishes.push_back(on_finish);
  }

  ret_code = 0;
  for (int i = 0; i < snap_count; ++i) {
    r = on_finishes[i]->wait();
    delete on_finishes[i];
    if (r < 0) {
      delete ictxs[i];
      ictxs[i] = nullptr;
      ret_code = r;
    }
  }
  if (ret_code != 0) {
    goto finish;
  }

  ne = cls::rbd::GroupSnapshotNamespace(group_ioctx.get_id(),
					group_id,
					group_snap.id);

  ldout(cct, 20) << "Opened participating images. Deleting snapshots themselves." << dendl;

  for (int i = 0; i < snap_count; ++i) {
    ImageCtx *ictx = ictxs[i];
    ldout(cct, 20) << "Removing individual snapshot with name: " << image_snap_name << dendl;
    on_finishes[i] = new C_SaferCond;

    std::string snap_name;
    ictx->snap_lock.get_read();
    snap_t snap_id = ictx->get_snap_id_from_namespace(ne);
    r = ictx->get_snap_name(snap_id, &snap_name);
    ictx->snap_lock.put_read();
    if (r >= 0) {
      ictx->operations->snap_remove(snap_name.c_str(), on_finishes[i]);
    }
    // We are ok to ignore missing image snapshots. The snapshot could have been inconsistent in the first place.
  }

  for (int i = 0; i < snap_count; ++i) {
    r = on_finishes[i]->wait();
    delete on_finishes[i];
    if (r < 0 && r != -ENOENT) { // if previous attempts to remove this snapshot failed then the image's snapshot may not exist
      lderr(cct) << "Failed deleting image snapshot. Ret code: " << r << dendl;
      ret_code = r;
    }
  }

  if (ret_code != 0) {
    goto finish;
  }

  ldout(cct, 20) << "Removed images snapshots removing snapshot record." << dendl;

  r = cls_client::group_snap_remove(&group_ioctx, group_header_oid, group_snap.id);
  if (r < 0) {
    ret_code = r;
    goto finish;
  }

finish:
  for (int i = 0; i < snap_count; ++i) {
    if (ictxs[i] != nullptr) {
      ictxs[i]->state->close();
    }
  }
  return ret_code;
}

int group_remove(librados::IoCtx& io_ctx, const char *group_name)
{
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "group_remove " << &io_ctx << " " << group_name << dendl;

  std::string group_id;
  int r = cls_client::dir_get_id(&io_ctx, RBD_GROUP_DIRECTORY,
				 std::string(group_name), &group_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error getting id of group" << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  std::vector<cls::rbd::GroupSnapshot> snaps;
  r = group_snap_list(io_ctx, group_name, &snaps);

  for (auto &snap : snaps) {
    r = group_snap_remove_by_record(io_ctx, snap, group_id, group_header_oid);
    if (r < 0) {
      return r;
    }
  }

  std::vector<cls::rbd::GroupImageStatus> images;
  r = group_image_list(io_ctx, group_name, &images);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error listing group images" << dendl;
    return r;
  }

  for (auto image : images) {
    librados::Rados rados(io_ctx);
    IoCtx image_ioctx;
    rados.ioctx_create2(image.spec.pool_id, image_ioctx);
    r = group_image_remove(io_ctx, group_id, image_ioctx, image.spec.image_id);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error removing image from a group" << dendl;
      return r;
    }
  }

  string header_oid = util::group_header_name(group_id);

  r = io_ctx.remove(header_oid);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing header: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::group_dir_remove(&io_ctx, RBD_GROUP_DIRECTORY,
				       group_name, group_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing group from directory" << dendl;
    return r;
  }

  return 0;
}

int group_list(IoCtx& io_ctx, vector<string> *names)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "group_list " << &io_ctx << dendl;

  int max_read = 1024;
  string last_read = "";
  int r;
  do {
    map<string, string> groups;
    r = cls_client::group_dir_list(&io_ctx, RBD_GROUP_DIRECTORY, last_read,
                                   max_read, &groups);
    if (r == -ENOENT) {
      return 0; // Ignore missing rbd group directory. It means we don't have any groups yet.
    }
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

int group_image_add(librados::IoCtx& group_ioctx, const char *group_name,
		    librados::IoCtx& image_ioctx, const char *image_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "group_image_add " << &group_ioctx
		 << " group name " << group_name << " image "
		 << &image_ioctx << " name " << image_name << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY, group_name, &group_id);
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

  r = cls_client::dir_get_id(&image_ioctx, RBD_DIRECTORY, image_name, &image_id);
  if (r < 0) {
    lderr(cct) << "error reading image id object: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  string image_header_oid = util::header_name(image_id);

  ldout(cct, 20) << "adding image " << image_name
		 << " image id " << image_header_oid << dendl;

  cls::rbd::GroupImageStatus incomplete_st(image_id, image_ioctx.get_id(),
				cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);
  cls::rbd::GroupImageStatus attached_st(image_id, image_ioctx.get_id(),
				  cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED);

  r = cls_client::group_image_set(&group_ioctx, group_header_oid,
				  incomplete_st);

  cls::rbd::GroupSpec group_spec(group_id, group_ioctx.get_id());

  if (r < 0) {
    lderr(cct) << "error adding image reference to consistency group: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::image_add_group(&image_ioctx, image_header_oid,
					 group_spec);
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

int group_image_remove(librados::IoCtx& group_ioctx, const char *group_name,
		       librados::IoCtx& image_ioctx, const char *image_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "group_remove_image " << &group_ioctx
		 << " group name " << group_name << " image "
		 << &image_ioctx << " name " << image_name << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY, group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }

  ldout(cct, 20) << "removing image from group name " << group_name
		 << " group id " << group_id << dendl;

  string image_id;
  r = cls_client::dir_get_id(&image_ioctx, RBD_DIRECTORY, image_name, &image_id);
  if (r < 0) {
    lderr(cct) << "error reading image id object: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = group_image_remove(group_ioctx, group_id, image_ioctx, image_id);

  return r;
}

int group_image_list(librados::IoCtx& group_ioctx,
		     const char *group_name,
		     std::vector<group_image_status_t>* images)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "group_image_list " << &group_ioctx
		 << " group name " << group_name << dendl;

  std::vector<cls::rbd::GroupImageStatus> image_ids;

  group_image_list(group_ioctx, group_name, &image_ids);

  for (auto image_id : image_ids) {
    librados::Rados rados(group_ioctx);
    IoCtx ioctx;
    rados.ioctx_create2(image_id.spec.pool_id, ioctx);
    std::string image_name;
    int r = cls_client::dir_get_name(&ioctx, RBD_DIRECTORY,
				     image_id.spec.image_id, &image_name);
    if (r < 0) {
      return r;
    }

    images->push_back(
	group_image_status_t {
	   image_name,
	   ioctx.get_pool_name(),
	   static_cast<group_image_state_t>(image_id.state)});
  }

  return 0;
}

int image_get_group(ImageCtx *ictx, group_spec_t *group_spec)
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
    group_spec->pool = ioctx.get_pool_name();
    group_spec->name = group_name;
  } else {
    group_spec->pool = "";
    group_spec->name = "";
  }

  return 0;
}

int group_snap_create(librados::IoCtx& group_ioctx,
		      const char *group_name, const char *snap_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);

  string group_id;
  cls::rbd::GroupSnapshot group_snap;
  vector<cls::rbd::ImageSnapshotRef> image_snaps;
  std::string ind_snap_name;

  std::vector<librbd::IoCtx*> io_ctxs;
  std::vector<librbd::ImageCtx*> ictxs;
  std::vector<C_SaferCond*> on_finishes;

  cls::rbd::SnapshotNamespace ne;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
				 group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }

  std::vector<cls::rbd::GroupImageStatus> images;
  r = group_image_list(group_ioctx, group_name, &images);
  if (r < 0) {
    return r;
  }
  int image_count = images.size();

  image_snaps = vector<cls::rbd::ImageSnapshotRef>(image_count, cls::rbd::ImageSnapshotRef());

  for (int i = 0; i < image_count; ++i) {
    image_snaps[i].pool = images[i].spec.pool_id;
    image_snaps[i].image_id = images[i].spec.image_id;
  }

  string group_header_oid = util::group_header_name(group_id);

  group_snap.id = generate_uuid(group_ioctx);
  group_snap.name = string(snap_name);
  group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_PENDING;
  group_snap.snaps = image_snaps;

  r = cls_client::group_snap_add(&group_ioctx, group_header_oid, group_snap);
  if (r == -EEXIST) {
    lderr(cct) << "snapshot with this name already exists: "
	       << cpp_strerror(r)
	       << dendl;
  }
  int ret_code = 0;
  if (r < 0) {
    ret_code = r;
    goto finish;
  }

  for (auto image: images) {
    librbd::IoCtx* image_io_ctx = new librbd::IoCtx;

    r = rados.ioctx_create2(image.spec.pool_id, *image_io_ctx);
    if (r < 0) {
      ldout(cct, 1) << "Failed to create io context for image" << dendl;
    }

    librbd::ImageCtx* image_ctx = new ImageCtx("", image.spec.image_id.c_str(),
					       nullptr, *image_io_ctx, false);

    C_SaferCond* on_finish = new C_SaferCond;

    image_ctx->state->open(on_finish);

    ictxs.push_back(image_ctx);
    on_finishes.push_back(on_finish);
  }
  ret_code = 0;
  for (int i = 0; i < image_count; ++i) {
    r = on_finishes[i]->wait();
    delete on_finishes[i];
    if (r < 0) {
      delete ictxs[i];
      ictxs[i] = nullptr;
      ret_code = r;
    }
  }
  if (ret_code != 0) {
    goto remove_record;
  }

  for (auto ictx: ictxs) {
    RWLock::RLocker owner_lock(ictx->owner_lock);
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->block_requests(-EBUSY);
    }
  }
  for (int i = 0; i < image_count; ++i) {
    ImageCtx *ictx = ictxs[i];
    RWLock::RLocker owner_lock(ictx->owner_lock);

    on_finishes[i] = new C_SaferCond;
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->request_lock(on_finishes[i]);
    }
  }

  ret_code = 0;
  for (int i = 0; i < image_count; ++i) {
    r = 0;
    ImageCtx *ictx = ictxs[i];
    if (ictx->exclusive_lock != nullptr) {
      r = on_finishes[i]->wait();
    }
    delete on_finishes[i];
    if (r < 0) {
      ret_code = r;
    }
  }
  if (ret_code != 0) {
    goto remove_record;
  }

  ind_snap_name = calc_ind_image_snap_name(group_ioctx.get_id(),
					   group_id,
					   group_snap.id);
  ne = cls::rbd::GroupSnapshotNamespace(group_ioctx.get_id(),
					group_id,
					group_snap.id);

  for (int i = 0; i < image_count; ++i) {
    ImageCtx *ictx = ictxs[i];

    C_SaferCond* on_finish = new C_SaferCond;

    ictx->operations->snap_create(ind_snap_name.c_str(), ne, on_finish);

    on_finishes[i] = on_finish;
  }

  ret_code = 0;
  for (int i = 0; i < image_count; ++i) {
    r = on_finishes[i]->wait();
    delete on_finishes[i];
    if (r < 0) {
      ret_code = r;
    } else {
      ImageCtx *ictx = ictxs[i];
      ictx->snap_lock.get_read();
      snap_t snap_id = ictx->get_snap_id_from_namespace(ne);
      ictx->snap_lock.put_read();
      if (snap_id == CEPH_NOSNAP) {
	ldout(cct, 20) << "Couldn't find supposedly created snapshot with namespace: " << ne << dendl;
	ret_code = -ENOENT;
      } else {
	image_snaps[i].snap_id = snapid_t(snap_id);
	image_snaps[i].pool = ictx->data_ctx.get_id();
	image_snaps[i].image_id = ictx->id;
      }
    }
  }
  if (ret_code != 0) {
    goto remove_image_snaps;
  }

  group_snap.snaps = image_snaps;
  group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;

  r = cls_client::group_snap_update(&group_ioctx, group_header_oid, group_snap);
  if (r < 0) {
    ret_code = r;
    goto remove_image_snaps;
  }

  goto finish;

remove_image_snaps:

  for (int i = 0; i < image_count; ++i) {
    ImageCtx *ictx = ictxs[i];
    ldout(cct, 20) << "Removing individual snapshot with name: " << ind_snap_name << dendl;
    on_finishes[i] = new C_SaferCond;
    std::string snap_name;
    ictx->snap_lock.get_read();
    snap_t snap_id = ictx->get_snap_id_from_namespace(ne);
    r = ictx->get_snap_name(snap_id, &snap_name);
    ictx->snap_lock.put_read();
    if (r >= 0) {
      ictx->operations->snap_remove(snap_name.c_str(), on_finishes[i]);
    }
    // Ignore missing image snapshots. The whole snapshot could have been inconsistent.
  }

  for (int i = 0; i < image_count; ++i) {
    r = on_finishes[i]->wait();
    delete on_finishes[i];
    if (r < 0 && r != -ENOENT) { // if previous attempts to remove this snapshot failed then the image's snapshot may not exist
      lderr(cct) << "Failed cleaning up image snapshot. Ret code: " << r << dendl;
      // just report error, but don't abort the process
    }
  }

remove_record:
  r = cls_client::group_snap_remove(&group_ioctx, group_header_oid, group_snap.id);
  if (r < 0) {
    lderr(cct) << "error while cleaning up group snapshot" << dendl;
    // we ignore return value in clean up
  }

finish:
  for (int i = 0; i < image_count; ++i) {
    if (ictxs[i] != nullptr) {
      ictxs[i]->state->close();
    }
  }
  return ret_code;
}

int group_snap_remove(librados::IoCtx& group_ioctx, const char *group_name,
		      const char *snap_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);

  std::vector<cls::rbd::GroupSnapshot> snaps;
  std::vector<C_SaferCond*> on_finishes;

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

  r = group_snap_list(group_ioctx, group_name, &snaps);
  if (r < 0) {
    return r;
  }
  cls::rbd::GroupSnapshot *group_snap = nullptr;
  for (auto &snap : snaps) {
    if (snap.name == string(snap_name)) {
      group_snap = &snap;
      break;
    }
  }
  if (group_snap == nullptr) {
    return -ENOENT;
  }

  r = group_snap_remove_by_record(group_ioctx, *group_snap, group_id, group_header_oid);

  return r;
}

int group_snap_info(librados::IoCtx& group_ioctx, const char *group_name,
		    const char *snap_name,
		    group_snap_spec_t *snap_spec)
{
  if (nullptr == snap_spec) {
    return -EINVAL;
  }

  cls::rbd::GroupSnapshot cls_snap;
  int r = group_snap_info(group_ioctx, group_name, snap_name, &cls_snap);
  if (r < 0) {
    return r;
  }
  snap_spec->name = cls_snap.name;
  snap_spec->state = static_cast<group_snap_state_t>(cls_snap.state);

  snap_spec->snaps.clear();
  for (cls::rbd::ImageSnapshotRef isr : cls_snap.snaps) {
    librados::Rados rados(group_ioctx);
    IoCtx ioctx;
    rados.ioctx_create2(isr.pool, ioctx);

    std::string image_name;
    int r = cls_client::dir_get_name(&ioctx, RBD_DIRECTORY,
				     isr.image_id, &image_name);
    if (r < 0) {
      return r;
    }

    group_image_snap_spec_t spec;
    spec.pool_name = ioctx.get_pool_name();
    spec.image_name = image_name;
    spec.snap_id = isr.snap_id;
    snap_spec->snaps.push_back(spec);
  }

  return 0;
}

int group_snap_list(librados::IoCtx& group_ioctx, const char *group_name,
		     std::vector<group_snap_spec_t> *snaps)
{
  std::vector<cls::rbd::GroupSnapshot> cls_snaps;

  int r = group_snap_list(group_ioctx, group_name, &cls_snaps);
  if (r < 0) {
    return r;
  }

  for (auto snap : cls_snaps) {
    snaps->push_back(
	group_snap_spec_t {
	   snap.name,
	   static_cast<group_snap_state_t>(snap.state)});

  }
  return 0;
}
} // namespace librbd
