
// ceph stuff
#include "Client.h"

#include "msg/CheesySerializer.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"


// unix-ey fs stuff
#include <sys/types.h>
#include <utime.h>


#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "client" << whoami << " "



// cons/des

Client::Client(MDCluster *mdc, int id, Messenger *m)
{
  mdcluster = mdc;
  whoami = id;

  // set up messengers
  messenger = m;
  messenger->set_dispatcher(this);

  all_files_closed = false;
  root = 0;

  set_cache_size(g_conf.client_cache_size);
}


Client::~Client() 
{
  if (messenger) { delete messenger; messenger = 0; }
}


void Client::init() {

}

void Client::shutdown() {

}

// -------------------
// fs ops
// namespace ops
//?int getdir(const char *path, fuse_dirh_t h, fuse_dirfil_t filler);
//int Client::link(const char *existing, const char *new)

int Client::unlink(const char *path)
{
  MClientRequest *req = new MClientRequest(MDS_OP_UNLINK, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "unlink result is " << res << endl;
  return res;
}

int Client::rename(const char *from, const char *to)
{
  MClientRequest *req = new MClientRequest(MDS_OP_RENAME, whoami);
  req->set_path(from);
  req->set_sarg(to);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "rename result is " << res << endl;
  return res;
}

// dirs

int Client::mkdir(const char *path, mode_t mode)
{
  MClientRequest *req = new MClientRequest(MDS_OP_MKDIR, whoami);
  req->set_path(path);
  req->set_iarg( (int)mode );
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "mkdir result is " << res << endl;
  return res;
}

int Client::rmdir(const char *path)
{
  MClientRequest *req = new MClientRequest(MDS_OP_RMDIR, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "rmdir result is " << res << endl;
  return res;
}

// symlinks
//int Client::readlink(const char *path, char *buf, size_t size)
  
int Client::symlink(const char *target, const char *link)
{
  MClientRequest *req = new MClientRequest(MDS_OP_SYMLINK, whoami);
  req->set_path(link);
  req->set_sarg(target);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "symlink result is " << res << endl;
  return res;
}



// inode stuff

int Client::lstat(const char *path, struct stat *stbuf)
// FIXME make sure this implements lstat and not stat
{
  MClientRequest *req = new MClientRequest(MDS_OP_STAT, whoami);
  req->set_path(path);
  
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  dout(10) << "lstat res is " << res << endl;
  if (res != 0) return res;
  
  //Transfer information from reply to stbuf
  vector<c_inode_info*> trace = reply->get_trace();
  inode_t inode = trace[trace.size()-1]->inode;
  //stbuf->st_dev = 
  stbuf->st_ino = inode.ino;
  stbuf->st_mode = inode.mode;
  //stbuf->st_nlink = 
  stbuf->st_uid = inode.uid;
  stbuf->st_gid = inode.gid;
  stbuf->st_ctime = inode.ctime;
  stbuf->st_atime = inode.atime;
  stbuf->st_mtime = inode.mtime;
  stbuf->st_size = (off_t) inode.size; //FIXME off_t is signed 64 vs size is unsigned 64
  //stbuf->st_blocks =
  //stbuf->st_blksize =
  //stbuf->st_flags =
  //stbuf->st_gen =

  // FIXME need to update cache with trace's inode_info
  delete reply;
  return 0;
}

int Client::chmod(const char *path, mode_t mode)
{
  MClientRequest *req = new MClientRequest(MDS_OP_CHMOD, whoami);
  req->set_path(path); 
  req->set_iarg( (int)mode );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "chmod result is " << res << endl;
  return res;
}

int Client::chown(const char *path, uid_t uid, gid_t gid)
{
  MClientRequest *req = new MClientRequest(MDS_OP_CHOWN, whoami);
  req->set_path(path); 
  req->set_iarg( (int)uid );
  req->set_iarg2( (int)gid );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?

  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "chown result is " << res << endl;
  return res;
}

int Client::utime(const char *path, struct utimbuf *buf)
{
  MClientRequest *req = new MClientRequest(MDS_OP_UTIME, whoami);
  req->set_path(path); 
  req->set_targ( buf->modtime );
  req->set_targ2( buf->actime );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();
  delete reply;
  dout(10) << "utime result is " << res << endl;
  return res;
}


  
//readdir usually include inode info for each entry except of locked entries

//
// getdir

// fyi: typedef int (*dirfillerfunc_t) (void *handle, const char *name, int type, inodeno_t ino);

int Client::getdir(const char *path, map<string,inode_t*>& contents) 
{
  MClientRequest *req = new MClientRequest(MDS_OP_READDIR, whoami);
  req->set_path(path); 

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  int res = reply->get_result();

  // dir contents to caller!
  vector<c_inode_info*>::iterator it;
  for (it = reply->get_dir_contents().begin(); 
	   it != reply->get_dir_contents().end(); 
	   it++) {
	// put in cache

    contents[(*it)->ref_dn] = &(*it)->inode;   // FIXME don't use one from reply!
  }

  //delete reply;     fix thing above first
  return res;
}



