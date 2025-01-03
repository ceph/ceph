// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"

#include "librbd/ImageCtx.h"
#include "librbd/LibrbdAdminSocketHook.h"
#include "librbd/internal.h"
#include "librbd/api/Io.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbdadminsocket: "

namespace librbd {

class LibrbdAdminSocketCommand {
public:
  virtual ~LibrbdAdminSocketCommand() {}
  virtual int call(Formatter *f) = 0;
};

class FlushCacheCommand : public LibrbdAdminSocketCommand {
public:
  explicit FlushCacheCommand(ImageCtx *ictx) : ictx(ictx) {}

  int call(Formatter *f) override {
    return api::Io<>::flush(*ictx);
  }

private:
  ImageCtx *ictx;
};

struct InvalidateCacheCommand : public LibrbdAdminSocketCommand {
public:
  explicit InvalidateCacheCommand(ImageCtx *ictx) : ictx(ictx) {}

  int call(Formatter *f) override {
    return invalidate_cache(ictx);
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

  r = admin_socket->register_command(command, this,
				     "flush rbd image " + imagename +
				     " cache");
  if (r == 0) {
    commands[command] = new FlushCacheCommand(ictx);
  }

  command = "rbd cache invalidate " + imagename;
  r = admin_socket->register_command(command, this,
				     "invalidate rbd image " + imagename + 
				     " cache");
  if (r == 0) {
    commands[command] = new InvalidateCacheCommand(ictx);
  }
}

LibrbdAdminSocketHook::~LibrbdAdminSocketHook() {
  (void)admin_socket->unregister_commands(this);
  for (Commands::const_iterator i = commands.begin(); i != commands.end();
       ++i) {
    delete i->second;
  }
}

int LibrbdAdminSocketHook::call(std::string_view command,
				const cmdmap_t& cmdmap,
				const bufferlist&,
				Formatter *f,
				std::ostream& errss,
				bufferlist& out) {
  Commands::const_iterator i = commands.find(command);
  ceph_assert(i != commands.end());
  return i->second->call(f);
}

} // namespace librbd
