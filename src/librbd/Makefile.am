librbd_types_la_SOURCES = \
	librbd/journal/Types.cc \
	librbd/WatchNotifyTypes.cc
noinst_LTLIBRARIES += librbd_types.la

if ENABLE_CLIENT
if WITH_RADOS
if WITH_RBD

librbd_internal_la_SOURCES = \
	librbd/AioCompletion.cc \
	librbd/AioImageRequest.cc \
	librbd/AioImageRequestWQ.cc \
	librbd/AioObjectRequest.cc \
	librbd/AsyncObjectThrottle.cc \
	librbd/AsyncOperation.cc \
	librbd/AsyncRequest.cc \
	librbd/CopyupRequest.cc \
	librbd/DiffIterate.cc \
	librbd/ExclusiveLock.cc \
	librbd/ImageCtx.cc \
	librbd/ImageState.cc \
	librbd/ImageWatcher.cc \
	librbd/internal.cc \
	librbd/Journal.cc \
	librbd/LibrbdAdminSocketHook.cc \
	librbd/LibrbdWriteback.cc \
	librbd/ObjectMap.cc \
	librbd/Operations.cc \
	librbd/Utils.cc \
	librbd/exclusive_lock/AcquireRequest.cc \
	librbd/exclusive_lock/ReleaseRequest.cc \
	librbd/image/CloseRequest.cc \
	librbd/image/OpenRequest.cc \
	librbd/image/RefreshParentRequest.cc \
	librbd/image/RefreshRequest.cc \
	librbd/image/SetSnapRequest.cc \
	librbd/image_watcher/Notifier.cc \
	librbd/image_watcher/NotifyLockOwner.cc \
	librbd/journal/Replay.cc \
	librbd/object_map/InvalidateRequest.cc \
	librbd/object_map/LockRequest.cc \
	librbd/object_map/Request.cc \
	librbd/object_map/RefreshRequest.cc \
	librbd/object_map/ResizeRequest.cc \
	librbd/object_map/SnapshotCreateRequest.cc \
	librbd/object_map/SnapshotRemoveRequest.cc \
	librbd/object_map/SnapshotRollbackRequest.cc \
	librbd/object_map/UnlockRequest.cc \
	librbd/object_map/UpdateRequest.cc \
	librbd/operation/FlattenRequest.cc \
	librbd/operation/RebuildObjectMapRequest.cc \
	librbd/operation/RenameRequest.cc \
	librbd/operation/Request.cc \
	librbd/operation/ResizeRequest.cc \
	librbd/operation/SnapshotCreateRequest.cc \
	librbd/operation/SnapshotProtectRequest.cc \
	librbd/operation/SnapshotRemoveRequest.cc \
	librbd/operation/SnapshotRenameRequest.cc \
	librbd/operation/SnapshotRollbackRequest.cc \
	librbd/operation/SnapshotUnprotectRequest.cc \
	librbd/operation/TrimRequest.cc
noinst_LTLIBRARIES += librbd_internal.la

librbd_api_la_SOURCES = \
	librbd/librbd.cc
noinst_LTLIBRARIES += librbd_api.la

librbd_la_SOURCES = \
	librbd/librbd.cc
librbd_la_LIBADD = \
	librbd_internal.la $(LIBRBD_TYPES) libjournal.la \
	$(LIBRADOS) $(LIBCOMMON) $(LIBOSDC) \
	librados_internal.la \
	libcls_rbd_client.la \
	libcls_lock_client.la \
	libcls_journal_client.la \
	$(PTHREAD_LIBS) $(EXTRALIBS)

librbd_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
librbd_la_CXXFLAGS = -fvisibility=hidden -fvisibility-inlines-hidden
librbd_la_LDFLAGS += -Xcompiler -Xlinker -Xcompiler '--exclude-libs=ALL'
endif
lib_LTLIBRARIES += librbd.la

noinst_HEADERS += \
	librbd/AioCompletion.h \
	librbd/AioImageRequest.h \
	librbd/AioImageRequestWQ.h \
	librbd/AioObjectRequest.h \
	librbd/AsyncObjectThrottle.h \
	librbd/AsyncOperation.h \
	librbd/AsyncRequest.h \
	librbd/CopyupRequest.h \
	librbd/DiffIterate.h \
	librbd/ExclusiveLock.h \
	librbd/ImageCtx.h \
	librbd/ImageState.h \
	librbd/ImageWatcher.h \
	librbd/internal.h \
	librbd/Journal.h \
	librbd/LibrbdAdminSocketHook.h \
	librbd/LibrbdWriteback.h \
	librbd/ObjectMap.h \
	librbd/Operations.h \
	librbd/parent_types.h \
	librbd/SnapInfo.h \
	librbd/TaskFinisher.h \
	librbd/Utils.h \
	librbd/WatchNotifyTypes.h \
	librbd/exclusive_lock/AcquireRequest.h \
	librbd/exclusive_lock/ReleaseRequest.h \
	librbd/image/CloseRequest.h \
	librbd/image/OpenRequest.h \
	librbd/image/RefreshParentRequest.h \
	librbd/image/RefreshRequest.h \
	librbd/image/SetSnapRequest.h \
	librbd/image_watcher/Notifier.h \
	librbd/image_watcher/NotifyLockOwner.h \
	librbd/journal/Replay.h \
	librbd/journal/Types.h \
	librbd/object_map/InvalidateRequest.h \
	librbd/object_map/LockRequest.h \
	librbd/object_map/Request.h \
	librbd/object_map/RefreshRequest.h \
	librbd/object_map/ResizeRequest.h \
	librbd/object_map/SnapshotCreateRequest.h \
	librbd/object_map/SnapshotRemoveRequest.h \
	librbd/object_map/SnapshotRollbackRequest.h \
	librbd/object_map/UnlockRequest.h \
	librbd/object_map/UpdateRequest.h \
	librbd/operation/FlattenRequest.h \
	librbd/operation/RebuildObjectMapRequest.h \
	librbd/operation/RenameRequest.h \
	librbd/operation/Request.h \
	librbd/operation/ResizeRequest.h \
	librbd/operation/SnapshotCreateRequest.h \
	librbd/operation/SnapshotProtectRequest.h \
	librbd/operation/SnapshotRemoveRequest.h \
	librbd/operation/SnapshotRenameRequest.h \
	librbd/operation/SnapshotRollbackRequest.h \
	librbd/operation/SnapshotUnprotectRequest.h \
	librbd/operation/TrimRequest.h

endif # WITH_RBD
endif # WITH_RADOS
endif # ENABLE_CLIENT
