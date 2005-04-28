
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
  messenger = m;
  serial_messenger = new CheesySerializer(m, this);

  all_files_closed = false;
  tid = 0;
  root = 0;

  set_cache_size(g_conf.client_cache_size);
}


Client::~Client() 
{
  if (messenger) { delete messenger; messenger = 0; }
  if (serial_messenger) { delete serial_messenger; serial_messenger = 0; }
}


void Client::init() {
  // incoming message go through serializer
  messenger->set_dispatcher(serial_messenger);
}

// -------------------
// fs ops

// inode stuff

int Client::lstat(const char *path, struct stat *stbuf)
// FIXME make sure this implements lstat and not stat
{
  MClientRequest *req = new MClientRequest(tid++, MDS_OP_STAT, whoami);
  MClientReply *reply;

  req->set_path(path);
  
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  reply = (MClientReply*)serial_messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  
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
  return 0;
}

int Client::chmod(const char *path, mode_t mode)
{
  MClientRequest *req = new MClientRequest(tid++, MDS_OP_CHMOD, whoami);
  MClientReply *reply;
  
  req->set_path(path); 

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  req->set_iarg( (int)mode );

  reply = (MClientReply*)serial_messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  dout(10) << "chmod result is " << reply->get_result() << endl;
  return reply->get_result();
}

int Client::chown(const char *path, uid_t uid, gid_t gid)
{
  MClientRequest *req = new MClientRequest(tid++, MDS_OP_CHOWN, whoami);
  MClientReply *reply;
  
  req->set_path(path); 

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  req->set_iarg( (int)uid );
  req->set_iarg2( (int)gid );

  reply = (MClientReply*)serial_messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  dout(10) << "chown result is " << reply->get_result() << endl;
  return reply->get_result();
}

int Client::utime(const char *path, struct utimbuf *buf)
{
  MClientRequest *req = new MClientRequest(tid++, MDS_OP_UTIME, whoami);
  MClientReply *reply;
  
  req->set_path(path); 

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  req->set_targ( buf->modtime );
  req->set_targ2( buf->actime );

  reply = (MClientReply*)serial_messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  dout(10) << "utime result is " << reply->get_result() << endl;
  return reply->get_result();
}


  
//readdir usually include inode info for each entry except of locked entries

//
// getdir

/*
typedef int (*fuse_dirfil_t) (fuse_dirh_t h, const char *name, int type,
                              ino_t ino);
*/
