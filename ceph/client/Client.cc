
// ceph stuff
#include "Client.h"

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
  dout(1) << "shutdown" << endl;
  messenger->shutdown();
}

// insert trace of reply into metadata cache

void Client::insert_trace(vector<c_inode_info*> trace)
{
  Inode *cur = root;

  for (int i=0; i<trace.size(); i++) {
	if (i == 0) {
	  if (!root) {
		cur = root = new Inode();
  	    root->inode = trace[i]->inode;
		inode_map[root->inode->ino] = root;
	  }
	  dout(12) << "insert_trace trace " << i << " root" << endl;
	} else {
	  Dir *dir = open_dir( cur );
	  string dname = trace[i]->ref_dn;
	  Dentry *next = NULL;
	  if (dir->dentries.count(dname))
	    next = dir->dentries[dname];
	  dout(12) << "insert_trace trace " << i << " dname " << dname << " ino " << trace[i]->inode.ino << endl;
	  
	  if (next) {
		if (next->inode.ino == trace[i]->inode.ino) {
		  touch_dn(next);
		  dout(12) << " had dentry " << dname << " with correct ino " << next->inode.ino << endl;
	    } else {
		  dout(12) << " had dentry " << dname << " with WRONG ino " << next->inode.ino << endl;
		  unlink(next);
		  next = NULL;
	    }
	  }
	  
	  if (!next) {
	    if (inode_map.count(trace[i]->inode.ino)) {
	      Inode *in = inode_map[trace[i]->inode.ino];
		  next = in->dn;
	 	  if (next) {
		    dout(12) << " had ino " << next->inode.ino << " at wrong position, moving" << endl;
		    if (next->dir)   // only if it was actually attached..
			  unlink(next);
		    next = link(dir, dname, in);
		  }
		}
	  }
	  
	  if (!next) {
	    next = link(dir, dname, new Inode());
		next->inode = trace[i]->inode;
		inode_map[next->inode->ino] = next->inode;
		dout(12) << " new dentry+node with ino " << next->inode.ino << endl;
	  }

	  cur = next->inode;
	}

	for (set<int>::iterator it = trace[i]->dist.begin(); it != trace[i]->dist.end(); it++)
	  cur->mds_contacts.push_back(*it);
  }
}

// -------------------
// fs ops
// namespace ops
//?int getdir(const char *path, fuse_dirh_t h, fuse_dirfil_t filler);
//int Client::link(const char *existing, const char *new)

int Client::unlink(const char *path)
{
  dout(3) << "unlink " << path << endl;
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
  dout(3) << "rename " << from << " " << to << endl;
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
  dout(3) << "mkdir " << path << " mode " << mode << endl;
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
  dout(3) << "rmdir " << path << endl;
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
  
int Client::symlink(const char *target, const char *link)
{
  dout(3) << "symlink target " << target << " link " << link << endl;
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

int Client::readlink(const char *path, char *buf, size_t size) 
{ 
  dout(3) << "readlink " << path << endl;
  // stat first  (OR, FIXME, use cached value?) ****
  struct stat stbuf;
  int r = this->lstat(path, &stbuf);
  if (r != 0) return r;

  // pull symlink content from cache
  Inode *in = inode_map[stbuf.st_ino];
  assert(in);  // i just did a stat
  
  // copy into buf (at most size bytes)
  int res = in->symlink->length();
  if (res > size) res = size;
  memcpy(buf, in->symlink->c_str(), res);

  return res;  // return length in bytes (to mimic the system call)
}



// inode stuff

int Client::lstat(const char *path, struct stat *stbuf)
// FIXME make sure this implements lstat and not stat
{
  dout(3) << "lstat " << path << endl;
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

  memset(stbuf, 0, sizeof(struct stat));
  //stbuf->st_dev = 
  stbuf->st_ino = inode.ino;
  stbuf->st_mode = inode.mode;
  stbuf->st_nlink = inode.nlink;
  stbuf->st_uid = inode.uid;
  stbuf->st_gid = inode.gid;
  stbuf->st_ctime = inode.ctime;
  stbuf->st_atime = inode.atime;
  stbuf->st_mtime = inode.mtime;
  stbuf->st_size = (off_t) inode.size; //FIXME off_t is signed 64 vs size is unsigned 64
  stbuf->st_blocks = (inode.size - 1) / 1024 + 1;
  stbuf->st_blksize = 1024;
  //stbuf->st_flags =
  //stbuf->st_gen =

  // gross HACK for now.. put an Inode in cache!  
  // for now i just need to put the symlink content somewhere (so that readlink() will work)
  Inode *in = new Inode;
  inode_map[inode.ino] = in;   // put in map so subsequent readlink will find it
  in->inode = inode;

  // symlink?
  if (in->inode.mode & INODE_MODE_SYMLINK) {
	if (!in->symlink) in->symlink = new string;
	*(in->symlink) = trace[trace.size()-1]->symlink;
  }

  dout(1) << "stat sez uid = " << inode.uid << " ino = " << stbuf->st_ino << endl;

  // FIXME need to update cache with trace's inode_info
  delete reply;
  return 0;
}



int Client::chmod(const char *path, mode_t mode)
{
  dout(3) << "chmod " << path << " mode " << mode << endl;
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
  dout(3) << "chown " << path << " " << uid << "." << gid << endl;
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
  dout(3) << "utime " << path << endl;
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
  dout(3) << "getdir " << path << endl;
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


// not written yet, but i want to link!

int Client::mknod(const char *path, mode_t mode) { }
int Client::link(const char *existing, const char *newname) {}
int Client::open(const char *path, int mode) {}
int Client::read(fileh_t fh, char *buf, size_t size, off_t offset) {}
int Client::write(fileh_t fh, const char *buf, size_t size, off_t offset) {}
int Client::statfs(const char *path, struct statfs *stbuf) {}
