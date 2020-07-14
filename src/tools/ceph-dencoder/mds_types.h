#ifdef WITH_CEPHFS
#include "mds/JournalPointer.h"
TYPE(JournalPointer)

#include "osdc/Journaler.h"
TYPE(Journaler::Header)

#include "mds/snap.h"
TYPE(SnapInfo)
TYPE(snaplink_t)
TYPE(sr_t)

#include "mds/mdstypes.h"
TYPE(frag_info_t)
TYPE(nest_info_t)
TYPE(quota_info_t)
TYPE(client_writeable_range_t)
TYPE_FEATUREFUL(inode_t<std::allocator>)
TYPE_FEATUREFUL(old_inode_t<std::allocator>)
TYPE(fnode_t)
TYPE(old_rstat_t)
TYPE_FEATUREFUL(session_info_t)
TYPE(string_snap_t)
TYPE(MDSCacheObjectInfo)
TYPE(mds_table_pending_t)
TYPE(cap_reconnect_t)
TYPE(inode_load_vec_t)
TYPE(dirfrag_load_vec_t)
TYPE(mds_load_t)
TYPE(MDSCacheObjectInfo)
TYPE(inode_backtrace_t)
TYPE(inode_backpointer_t)

#include "mds/CInode.h"
TYPE_FEATUREFUL(InodeStore)
TYPE_FEATUREFUL(InodeStoreBare)

#include "mds/MDSMap.h"
TYPE_FEATUREFUL(MDSMap)
TYPE_FEATUREFUL(MDSMap::mds_info_t)

#include "mds/FSMap.h"
//TYPE_FEATUREFUL(Filesystem)
TYPE_FEATUREFUL(FSMap)

#include "mds/Capability.h"
TYPE_NOCOPY(Capability)

#include "mds/inode_backtrace.h"
TYPE(inode_backpointer_t)
TYPE(inode_backtrace_t)

#include "mds/InoTable.h"
TYPE(InoTable)

#include "mds/SnapServer.h"
TYPE_STRAYDATA(SnapServer)

#include "mds/events/ECommitted.h"
TYPE_FEATUREFUL_NOCOPY(ECommitted)

#include "mds/events/EExport.h"
TYPE_FEATUREFUL_NOCOPY(EExport)

#include "mds/events/EFragment.h"
TYPE_FEATUREFUL_NOCOPY(EFragment)

#include "mds/events/EImportFinish.h"
TYPE_FEATUREFUL_NOCOPY(EImportFinish)

#include "mds/events/EImportStart.h"
TYPE_FEATUREFUL_NOCOPY(EImportStart)

#include "mds/events/EMetaBlob.h"
TYPE_FEATUREFUL_NOCOPY(EMetaBlob::fullbit)
TYPE(EMetaBlob::remotebit)
TYPE(EMetaBlob::nullbit)
TYPE_FEATUREFUL_NOCOPY(EMetaBlob::dirlump)
TYPE_FEATUREFUL_NOCOPY(EMetaBlob)

#include "mds/events/EOpen.h"
TYPE_FEATUREFUL_NOCOPY(EOpen)

#include "mds/events/EResetJournal.h"
TYPE_FEATUREFUL_NOCOPY(EResetJournal)

#include "mds/events/ESession.h"
TYPE_FEATUREFUL_NOCOPY(ESession)

#include "mds/events/ESessions.h"
TYPE_FEATUREFUL_NOCOPY(ESessions)

#include "mds/events/EPeerUpdate.h"
TYPE(link_rollback)
TYPE(rmdir_rollback)
TYPE(rename_rollback::drec)
TYPE(rename_rollback)
TYPE_FEATUREFUL_NOCOPY(EPeerUpdate)

#include "mds/events/ESubtreeMap.h"
TYPE_FEATUREFUL_NOCOPY(ESubtreeMap)

#include "mds/events/ETableClient.h"
TYPE_FEATUREFUL_NOCOPY(ETableClient)

#include "mds/events/ETableServer.h"
TYPE_FEATUREFUL_NOCOPY(ETableServer)

#include "mds/events/EUpdate.h"
TYPE_FEATUREFUL_NOCOPY(EUpdate)
#endif // WITH_CEPHFS
