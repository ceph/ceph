/*
 * Example live patch for ceph-mds, see doc/dev/livepatch.rst: a replacement
 * for Server::check_dir_max_entries().
 *
 *   build-patch.sh <build dir> lp_dir_max_entries.cc ceph-mds \
 *     mds/Server.cc 'Server21check_dir_max_entries' \
 *     lp_Server_check_dir_max_entries
 *
 * Directories are capped at LP_DIR_CAP entries, whatever
 * mds_dir_max_entries says. A member function takes `this` as its first
 * argument, so a free function with the same remaining arguments has the
 * same calling convention. Built with -fno-access-control to reach
 * private members.
 */
#include "mds/Server.h"
#include "mds/MDSRank.h"
#include "mds/CDir.h"
#include "mds/CInode.h"
#include "mds/Mutation.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << self->mds->get_nodeid() << ".server(livepatch) "

static constexpr uint64_t LP_DIR_CAP = 10;

extern "C" bool lp_Server_check_dir_max_entries(Server *self,
                                                 const MDRequestRef& mdr,
                                                 CDir *in)
{
  const uint64_t size = in->inode->get_projected_inode()->dirstat.nfiles +
                        in->inode->get_projected_inode()->dirstat.nsubdirs;
  const uint64_t max = self->dir_max_entries ?
    std::min<uint64_t>(self->dir_max_entries, LP_DIR_CAP) : LP_DIR_CAP;
  if (size >= max) {
    dout(1) << "entries per dir " << *in << " size exceeds " << max
            << " (ENOSPC)" << dendl;
    self->respond_to_request(mdr, -ENOSPC);
    return false;
  }
  return true;
}
