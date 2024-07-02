#include "ESession.h"
#include "ESessions.h"
#include "ESubtreeMap.h"

#include "EMetaBlob.h"
#include "ENoOp.h"
#include "EResetJournal.h"

#include "ECommitted.h"
#include "EOpen.h"
#include "EPeerUpdate.h"
#include "EPurged.h"
#include "EUpdate.h"

#include "EExport.h"
#include "EFragment.h"
#include "EImportFinish.h"
#include "EImportStart.h"

#include "ELid.h"
#include "ESegment.h"
#include "ETableClient.h"
#include "ETableServer.h"


void EMetaBlob::fullbit::generate_test_instances(std::list<EMetaBlob::fullbit*>& ls)
{
  auto _inode = CInode::allocate_inode();
  fragtree_t fragtree;
  auto _xattrs = CInode::allocate_xattr_map();
  bufferlist empty_snapbl;
  fullbit *sample = new fullbit("/testdn", "", 0, 0, 0,
                                _inode, fragtree, _xattrs, "", 0, empty_snapbl,
                                false, NULL);
  ls.push_back(sample);
}

void EMetaBlob::remotebit::
generate_test_instances(std::list<EMetaBlob::remotebit*>& ls)
{
  remotebit *remote = new remotebit("/test/dn", "", 0, 10, 15, 1, IFTODT(S_IFREG), false);
  ls.push_back(remote);
  remote = new remotebit("/test/dn2", "foo", 0, 10, 15, 1, IFTODT(S_IFREG), false);
  ls.push_back(remote);
}

void EMetaBlob::nullbit::generate_test_instances(std::list<nullbit*>& ls)
{
  nullbit *sample = new nullbit("/test/dentry", 0, 10, 15, false);
  nullbit *sample2 = new nullbit("/test/dirty", 10, 20, 25, true);
  ls.push_back(sample);
  ls.push_back(sample2);
}

void EMetaBlob::dirlump::generate_test_instances(std::list<dirlump*>& ls)
{
  auto dl = new dirlump();
  dl->fnode = CDir::allocate_fnode();
  ls.push_back(dl);
}

void EMetaBlob::generate_test_instances(std::list<EMetaBlob*>& ls)
{
  ls.push_back(new EMetaBlob());
}

void ESession::generate_test_instances(std::list<ESession*>& ls)
{
  ls.push_back(new ESession);
}

void ESessions::generate_test_instances(std::list<ESessions*>& ls)
{
  ls.push_back(new ESessions());
}

void ETableServer::generate_test_instances(std::list<ETableServer*>& ls)
{
  ls.push_back(new ETableServer());
}

void ETableClient::generate_test_instances(std::list<ETableClient*>& ls)
{
  ls.push_back(new ETableClient());
}

void EUpdate::generate_test_instances(std::list<EUpdate*>& ls)
{
  ls.push_back(new EUpdate());
}

void EOpen::generate_test_instances(std::list<EOpen*>& ls)
{
  ls.push_back(new EOpen());
  ls.push_back(new EOpen());
  ls.back()->add_ino(0);
}

void ECommitted::generate_test_instances(std::list<ECommitted*>& ls)
{
  ls.push_back(new ECommitted);
  ls.push_back(new ECommitted);
  ls.back()->stamp = utime_t(1, 2);
  ls.back()->reqid = metareqid_t(entity_name_t::CLIENT(123), 456);
}

void EFragment::generate_test_instances(std::list<EFragment*>& ls)
{
  ls.push_back(new EFragment);
  ls.push_back(new EFragment);
  ls.back()->op = OP_PREPARE;
  ls.back()->ino = 1;
  ls.back()->bits = 5;
}

void EExport::generate_test_instances(std::list<EExport*>& ls)
{
  EExport *sample = new EExport();
  ls.push_back(sample);
}

void ESegment::generate_test_instances(std::list<ESegment*>& ls)
{
  ls.push_back(new ESegment);
}

void ELid::generate_test_instances(std::list<ELid*>& ls)
{
  ls.push_back(new ELid);
}

void link_rollback::generate_test_instances(std::list<link_rollback*>& ls)
{
  ls.push_back(new link_rollback());
}


void rmdir_rollback::generate_test_instances(std::list<rmdir_rollback*>& ls)
{
  ls.push_back(new rmdir_rollback());
}


void rename_rollback::drec::generate_test_instances(std::list<drec*>& ls)
{
  ls.push_back(new drec());
  ls.back()->remote_d_type = IFTODT(S_IFREG);
}

void rename_rollback::generate_test_instances(std::list<rename_rollback*>& ls)
{
  ls.push_back(new rename_rollback());
  ls.back()->orig_src.remote_d_type = IFTODT(S_IFREG);
  ls.back()->orig_dest.remote_d_type = IFTODT(S_IFREG);
  ls.back()->stray.remote_d_type = IFTODT(S_IFREG);
}

void EPeerUpdate::generate_test_instances(std::list<EPeerUpdate*>& ls)
{
  ls.push_back(new EPeerUpdate());
}

void ESubtreeMap::generate_test_instances(std::list<ESubtreeMap*>& ls)
{
  ls.push_back(new ESubtreeMap());
}

void EImportStart::generate_test_instances(std::list<EImportStart*>& ls)
{
  ls.push_back(new EImportStart);
}

void EResetJournal::generate_test_instances(std::list<EResetJournal*>& ls)
{
  ls.push_back(new EResetJournal());
}
