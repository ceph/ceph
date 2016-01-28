// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"

#include "librbd/ImageCtx.h"
#include "librbd/LibrbdAdminSocketHook.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbdadminsocket: "

namespace librbd {

class LibrbdAdminSocketCommand {
public:
  virtual ~LibrbdAdminSocketCommand() {}
  virtual bool call(stringstream *ss) = 0;
};

class FlushCacheCommand : public LibrbdAdminSocketCommand {
public:
  explicit FlushCacheCommand(ImageCtx *ictx) : ictx(ictx) {}

  bool call(stringstream *ss) {
    int r = flush(ictx);
    if (r < 0) {
      *ss << "flush: " << cpp_strerror(r);
      return false;
    }
    return true;
  }

private:
  ImageCtx *ictx;
};

struct InvalidateCacheCommand : public LibrbdAdminSocketCommand {
public:
  explicit InvalidateCacheCommand(ImageCtx *ictx) : ictx(ictx) {}

  bool call(stringstream *ss) {
    int r = invalidate_cache(ictx);
    if (r < 0) {
      *ss << "invalidate_cache: " << cpp_strerror(r);
      return false;
    }
    return true;
  }

private:
  ImageCtx *ictx;
};

LibrbdAdminSocketHook::LibrbdAdminSocketHook(ImageCtx *ictx) :
  admin_socket(ictx->cct->get_admin_socket()) {

  std::string command;
  std::string imagename;
  int r;

  imagename = ictx->md_ctx.get_pool_name() + "/" + ictx->name;
  command = "rbd cache flush " + imagename;

  r = admin_socket->register_command(command, command, this,
				     "flush rbd image " + imagename +
				     " cache");
  if (r == 0) {
    commands[command] = new FlushCacheCommand(ictx);
  }

  command = "rbd cache invalidate " + imagename;
  r = admin_socket->register_command(command, command, this,
				     "invalidate rbd image " + imagename + 
				     " cache");
  if (r == 0) {
    commands[command] = new InvalidateCacheCommand(ictx);
  }
}

LibrbdAdminSocketHook::~LibrbdAdminSocketHook() {
  for (Commands::const_iterator i = commands.begin(); i != commands.end();
       ++i) {
    (void)admin_socket->unregister_command(i->first);
    delete i->second;
  }
}

bool LibrbdAdminSocketHook::call(std::string command, cmdmap_t& cmdmap,
				 std::string format, bufferlist& out) {
  Commands::const_iterator i = commands.find(command);
  assert(i != commands.end());
  stringstream ss;
  bool r = i->second->call(&ss);
  out.append(ss);
  return r;
}

} // namespace librbd
