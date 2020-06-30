#ifdef WITH_RBD
#include "librbd/journal/Types.h"
TYPE(librbd::journal::EventEntry)
TYPE(librbd::journal::ClientData)
TYPE(librbd::journal::TagData)
#include "librbd/mirroring_watcher/Types.h"
TYPE(librbd::mirroring_watcher::NotifyMessage)
#include "librbd/trash_watcher/Types.h"
TYPE(librbd::mirroring_watcher::NotifyMessage)
#include "librbd/WatchNotifyTypes.h"
TYPE_NOCOPY(librbd::watch_notify::NotifyMessage)
TYPE(librbd::watch_notify::ResponseMessage)

#include "rbd_replay/ActionTypes.h"
TYPE(rbd_replay::action::Dependency)
TYPE(rbd_replay::action::ActionEntry)

#include "tools/rbd_mirror/image_map/Types.h"
TYPE(rbd::mirror::image_map::PolicyData)
#endif

#ifdef WITH_RBD
#include "cls/rbd/cls_rbd.h"
TYPE_FEATUREFUL(cls_rbd_parent)
TYPE_FEATUREFUL(cls_rbd_snap)

#include "cls/rbd/cls_rbd_types.h"
TYPE(cls::rbd::ParentImageSpec)
TYPE(cls::rbd::ChildImageSpec)
TYPE(cls::rbd::MigrationSpec)
TYPE(cls::rbd::MirrorPeer)
TYPE(cls::rbd::MirrorImage)
TYPE(cls::rbd::MirrorImageMap)
TYPE(cls::rbd::MirrorImageStatus)
TYPE(cls::rbd::MirrorImageSiteStatus)
TYPE_FEATUREFUL(cls::rbd::MirrorImageSiteStatusOnDisk)
TYPE(cls::rbd::GroupImageSpec)
TYPE(cls::rbd::GroupImageStatus)
TYPE(cls::rbd::GroupSnapshot)
TYPE(cls::rbd::GroupSpec)
TYPE(cls::rbd::ImageSnapshotSpec)
TYPE(cls::rbd::SnapshotInfo)
TYPE(cls::rbd::SnapshotNamespace)
#endif
