
// ceph stuff
#include "Client.h"

// unix-ey fs stuff
#include <sys/types.h>
#include <time.h>
#include <utime.h>

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientFileCaps.h"

#include "messages/MGenericMessage.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"

#include "osd/Filer.h"

#include "common/Cond.h"
#include "common/Mutex.h"

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "client" << whoami << " "



// cons/des

Client::Client(MDCluster *mdc, int id, Messenger *m)
{
  mdcluster = mdc;
  whoami = id;

  mounted = false;

  // 
  all_files_closed = false;
  root = 0;

  set_cache_size(g_conf.client_cache_size);

  // set up messengers
  messenger = m;
  messenger->set_dispatcher(this);

  // osd interfaces
  osdcluster = new OSDCluster();     // initially blank.. see mount()
  filer = new Filer(messenger, osdcluster);
}


Client::~Client() 
{
  if (messenger) { delete messenger; messenger = 0; }
  if (filer) { delete filer; filer = 0; }
  if (osdcluster) { delete osdcluster; osdcluster = 0; }
}


void Client::init() {
  
}

void Client::shutdown() {
  dout(1) << "shutdown" << endl;
  messenger->shutdown();
}




// ===================
// metadata cache stuff


// insert inode info into metadata cache

Inode* Client::insert_inode_info(Dir *dir, c_inode_info *in_info)
{
  string dname = in_info->ref_dn;
  Dentry *dn = NULL;
  if (dir->dentries.count(dname))
	dn = dir->dentries[dname];
  dout(12) << "insert_inode_info " << dname << " ino " << in_info->inode.ino << "  size " << in_info->inode.size << endl;
  
  if (dn) {
	if (dn->inode->inode.ino == in_info->inode.ino) {
	  touch_dn(dn);
	  dout(12) << " had dentry " << dname << " with correct ino " << dn->inode->inode.ino << endl;
	} else {
	  dout(12) << " had dentry " << dname << " with WRONG ino " << dn->inode->inode.ino << endl;
	  unlink(dn);
	  dn = NULL;
	}
  }
  
  if (!dn) {
	if (inode_map.count(in_info->inode.ino)) {
	  Inode *in = inode_map[in_info->inode.ino];
	  if (in) {
		dout(12) << " had ino " << in->inode.ino << " at wrong position, moving" << endl;
		if (in->dn) unlink(in->dn);
		dn = link(dir, dname, in);
	  }
	}
  }
  
  if (!dn) {
	Inode *in = new Inode;
	inode_map[in_info->inode.ino] = in;
	dn = link(dir, dname, in);
	dout(12) << " new dentry+node with ino " << in_info->inode.ino << endl;
  }

  // OK!
  assert(dn && dn->inode);

  // actually update info
  dn->inode->inode = in_info->inode;

  // symlink?
  if ((dn->inode->inode.mode & INODE_TYPE_MASK) == INODE_MODE_SYMLINK) {
	if (!dn->inode->symlink) 
	  dn->inode->symlink = new string;
	*(dn->inode->symlink) = in_info->symlink;
  }

  // take note of latest distribution on mds's
  dn->inode->mds_contacts = in_info->dist;

  return dn->inode;
}

// insert trace of reply into metadata cache

void Client::insert_trace(const vector<c_inode_info*>& trace)
{
  Inode *cur = root;
  time_t now = time(NULL);

  if (trace.empty()) return;
  
  for (int i=0; i<trace.size(); i++) {
    if (i == 0) {
      if (!root) {
        cur = root = new Inode();
        root->inode = trace[i]->inode;
        inode_map[root->inode.ino] = root;
      }
	  root->mds_contacts = trace[i]->dist;
	  root->last_updated = now;
      dout(12) << "insert_trace trace " << i << " root" << endl;
    } else {
      dout(12) << "insert_trace trace " << i << endl;
      Dir *dir = open_dir( cur );
	  cur = this->insert_inode_info(dir, trace[i]);
	  cur->last_updated = now;
    }    
  }
}




Dentry *Client::lookup(filepath& path)
{
  dout(14) << "lookup " << path << endl;

  Inode *cur = root;
  if (!cur) return NULL;

  Dentry *dn = 0;
  for (int i=0; i<path.depth(); i++) {
	dout(14) << " seg " << i << " = " << path[i] << endl;
	if (cur->inode.mode & INODE_MODE_DIR) {
	  // dir, we can descend
	  Dir *dir = open_dir(cur);
	  if (dir->dentries.count(path[i])) {
		dn = dir->dentries[path[i]];
		dout(14) << " hit dentry " << path[i] << " inode is " << dn->inode << " last_updated " << dn->inode->last_updated<< endl;
	  } else {
		dout(14) << " dentry " << path[i] << " dne" << endl;
		return NULL;
	  }
	  cur = dn->inode;
	  assert(cur);
	} else {
	  return NULL;  // not a dir
	}
  }
  
  if (dn) {
	dout(11) << "lookup '" << path << "' found " << dn->name << " inode " << dn->inode->inode.ino << " last_updated " << dn->inode->last_updated<< endl;
  }

  return dn;
}




// -------

MClientReply *Client::make_request(MClientRequest *req, int mds)
{
  // drop mutex for duration of call
  client_lock.Unlock();  
  MClientReply *reply = (MClientReply*)messenger->sendrecv(req,
														   MSG_ADDR_MDS(mds), 
														   MDS_PORT_SERVER);
  client_lock.Lock();  
  return reply;
}



// ------------------------
// incoming messages

void Client::dispatch(Message *m)
{
  client_lock.Lock();

  switch (m->get_type()) {
	// osd
  case MSG_OSD_READREPLY:
	filer->handle_osd_read_reply((MOSDReadReply*)m);
	break;
  case MSG_OSD_WRITEREPLY:
	filer->handle_osd_write_reply((MOSDWriteReply*)m);
	break;
  case MSG_OSD_OPREPLY:
	filer->handle_osd_op_reply((MOSDOpReply*)m);
	break;
	
	// client
  case MSG_CLIENT_FILECAPS:
	handle_file_caps((MClientFileCaps*)m);
	break;

  default:
	cout << "dispatch doesn't recognize message type " << m->get_type() << endl;
	assert(0);  // fail loudly
	break;
  }

  client_lock.Unlock();
}

void Client::handle_file_caps(MClientFileCaps *m)
{
  Fh *f = fh_map[m->get_fh()];
  
  if (f && fh_closing.count(m->get_fh()) == 0) {
	// file is still open, we care

	// auth?
	if (m->get_mds() >= 0) {
	  f->mds = m->get_mds();
	  dout(5) << "handle_file_caps on fh " << m->get_fh() << " mds now " << f->mds << endl;
	}
	
	f->caps = m->get_caps();
	dout(5) << "handle_file_caps on fh " << m->get_fh() << " caps now " << f->caps << endl;
	
	// update inode
	Inode *in = inode_map[f->ino];
	assert(in);
	in->inode = m->get_inode();      // might have updated size... FIXME this is overkill!
	
	// ack?
	if (m->needs_ack()) {
	  dout(5) << "acking" << endl;
	  messenger->send_message(m, m->get_source(), m->get_source_port());
	  return;
	} 
  } else {
	dout(5) << "handle_file_caps fh is closed or closing, dropping" << endl;
  }

  delete m;
}




// -------------------
// fs ops

int Client::mount(int mkfs)
{
  client_lock.Lock();

  assert(!mounted);  // caller is confused?

  dout(1) << "mounting" << endl;
  MClientMount *m = new MClientMount();
  if (mkfs) m->set_mkfs(mkfs);

  client_lock.Unlock();
  MClientMountAck *reply = (MClientMountAck*)messenger->sendrecv(m,
																 MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  client_lock.Lock();
  assert(reply);

  // we got osdcluster!
  int off = 0;
  osdcluster->_unrope(reply->get_osd_cluster_state(), off);

  dout(1) << "mounted" << endl;
  mounted = true;

  client_lock.Unlock();
}

int Client::unmount()
{
  client_lock.Lock();

  assert(mounted);  // caller is confused?

  dout(1) << "unmounting" << endl;
  Message *req = new MGenericMessage(MSG_CLIENT_UNMOUNT);
  client_lock.Unlock();
  Message *reply = messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  client_lock.Lock();
  assert(reply);
  mounted = false;
  dout(1) << "unmounted" << endl;

  client_lock.Unlock();
}



// namespace ops

int Client::link(const char *existing, const char *newname) 
{
  client_lock.Lock();

  // main path arg is new link name
  // sarg is target (existing file)

  dout(3) << "link " << existing << " " << newname << endl;

  MClientRequest *req = new MClientRequest(MDS_OP_LINK, whoami);
  req->set_path(newname);
  req->set_sarg(existing);
  
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "link result is " << res << endl;

  client_lock.Unlock();
  return res;
}


int Client::unlink(const char *path)
{
  client_lock.Lock();

  dout(3) << "unlink " << path << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_UNLINK, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  if (res == 0) {
	//this crashes, haven't looked at why yet
	//Dentry *dn = lookup(req->get_filepath()); 
	//if (dn) unlink(dn);
  }
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "unlink result is " << res << endl;

  client_lock.Unlock();
  return res;
}

int Client::rename(const char *from, const char *to)
{
  client_lock.Lock();

  dout(3) << "rename " << from << " " << to << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_RENAME, whoami);
  req->set_path(from);
  req->set_sarg(to);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "rename result is " << res << endl;

  client_lock.Unlock();
  return res;
}

// dirs

int Client::mkdir(const char *path, mode_t mode)
{
  client_lock.Lock();

  dout(3) << "mkdir " << path << " mode " << mode << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_MKDIR, whoami);
  req->set_path(path);
  req->set_iarg( (int)mode );
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "mkdir result is " << res << endl;

  client_lock.Unlock();
  return res;
}

int Client::rmdir(const char *path)
{
  client_lock.Lock();

  dout(3) << "rmdir " << path << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_RMDIR, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  if (res == 0) {
	// crashes, not sure why yet
	//unlink(lookup(req->get_filepath()));
  }
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "rmdir result is " << res << endl;

  client_lock.Unlock();
  return res;
}

// symlinks
  
int Client::symlink(const char *target, const char *link)
{
  client_lock.Lock();

  dout(3) << "symlink target " << target << " link " << link << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_SYMLINK, whoami);
  req->set_path(link);
  req->set_sarg(target);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  //FIXME assuming trace of link, not of target
  delete reply;
  dout(10) << "symlink result is " << res << endl;

  client_lock.Unlock();
  return res;
}

int Client::readlink(const char *path, char *buf, size_t size) 
{ 
  dout(3) << "readlink " << path << endl;
  // stat first  (FIXME, PERF access cache directly) ****
  struct stat stbuf;
  int r = this->lstat(path, &stbuf);
  if (r != 0) return r;

  client_lock.Lock();

  // pull symlink content from cache
  Inode *in = inode_map[stbuf.st_ino];
  assert(in);  // i just did a stat
  
  // copy into buf (at most size bytes)
  int res = in->symlink->length();
  if (res > size) res = size;
  memcpy(buf, in->symlink->c_str(), res);

  client_lock.Unlock();
  return res;  // return length in bytes (to mimic the system call)
}



// inode stuff

int Client::lstat(const char *path, struct stat *stbuf)
{
  client_lock.Lock();

  dout(3) << "lstat " << path << endl;
  // FIXME, PERF request allocation convenient but not necessary for cache hit
  MClientRequest *req = new MClientRequest(MDS_OP_STAT, whoami);
  req->set_path(path);
  
  // check whether cache content is fresh enough
  int res = 0;
  Dentry *dn = lookup(req->get_filepath());
  inode_t inode;
  time_t now = time(NULL);
  if (dn && ((now - dn->inode->last_updated) <= g_conf.client_cache_stat_ttl)) {
	inode = dn->inode->inode;
	dout(10) << "lstat cache hit, age is " << (now - dn->inode->last_updated) << endl;
  } else {  
	// FIXME where does FUSE maintain user information
	req->set_caller_uid(getuid());
	req->set_caller_gid(getgid());
	
	MClientReply *reply = make_request(req, 0);
	res = reply->get_result();
	dout(10) << "lstat res is " << res << endl;
	if (res == 0) {
	  //Transfer information from reply to stbuf
	  vector<c_inode_info*> trace = reply->get_trace();
	  inode = trace[trace.size()-1]->inode;
	  
	  //Update metadata cache
	  this->insert_trace(trace);
	  delete reply;
	}
  }
     
  if (res == 0) {
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
	
	dout(10) << "stat sez size = " << inode.size << "   uid = " << inode.uid << " ino = " << stbuf->st_ino << endl;
  }

  client_lock.Unlock();
  return res;
}



int Client::chmod(const char *path, mode_t mode)
{
  client_lock.Lock();

  dout(3) << "chmod " << path << " mode " << mode << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_CHMOD, whoami);
  req->set_path(path); 
  req->set_iarg( (int)mode );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "chmod result is " << res << endl;

  client_lock.Unlock();
  return res;
}

int Client::chown(const char *path, uid_t uid, gid_t gid)
{
  client_lock.Lock();

  dout(3) << "chown " << path << " " << uid << "." << gid << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_CHOWN, whoami);
  req->set_path(path); 
  req->set_iarg( (int)uid );
  req->set_iarg2( (int)gid );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?

  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "chown result is " << res << endl;

  client_lock.Unlock();
  return res;
}

int Client::utime(const char *path, struct utimbuf *buf)
{
  client_lock.Lock();

  dout(3) << "utime " << path << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_UTIME, whoami);
  req->set_path(path); 
  req->set_targ( buf->modtime );
  req->set_targ2( buf->actime );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "utime result is " << res << endl;

  client_lock.Unlock();
  return res;
}



int Client::mknod(const char *path, mode_t mode) 
{ 
  client_lock.Lock();

  dout(3) << "mknod " << path << " mode " << mode << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_MKNOD, whoami);
  req->set_path(path); 
  req->set_iarg( mode );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  

  size_t size = inode_map[ reply->get_trace()[reply->get_trace().size()-1]->inode.ino ]->inode.size;

  delete reply;
  dout(10) << "mknod result is " << res << ", size is " << size << endl;

  client_lock.Unlock();
  return res;
}



  
//readdir usually include inode info for each entry except of locked entries

//
// getdir

// fyi: typedef int (*dirfillerfunc_t) (void *handle, const char *name, int type, inodeno_t ino);

int Client::getdir(const char *path, map<string,inode_t*>& contents) 
{
  client_lock.Unlock();

  dout(3) << "getdir " << path << endl;
  MClientRequest *req = new MClientRequest(MDS_OP_READDIR, whoami);
  req->set_path(path); 

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, 0);
  int res = reply->get_result();
  vector<c_inode_info*> trace = reply->get_trace();
  this->insert_trace(trace);  

  if (res == 0) {

	// dir contents to cache!
	inodeno_t ino = trace[trace.size()-1]->inode.ino;
	Inode *diri = inode_map[ ino ];
	assert(diri);
	assert(diri->inode.mode & INODE_MODE_DIR);
	Dir *dir = open_dir(diri);
	assert(dir);
	time_t now = time(NULL);
	for (vector<c_inode_info*>::iterator it = reply->get_dir_contents().begin(); 
		 it != reply->get_dir_contents().end(); 
		 it++) {
	  // put in cache
	  Inode *in = this->insert_inode_info(dir, *it);
	  in->last_updated = now;
	  
	  // contents to caller too!
	  contents[(*it)->ref_dn] = &in->inode;
	}

	// FIXME: remove items in cache that weren't in my readdir
	// ***
  }

  delete reply;     //fix thing above first

  client_lock.Unlock();
  return res;
}




/****** file i/o **********/

int Client::open(const char *path, int mode) 
{
  client_lock.Lock();

  dout(3) << "open " << path << " mode " << mode << endl;
  
  MClientRequest *req = new MClientRequest(MDS_OP_OPEN, whoami);
  req->set_path(path); 
  req->set_iarg(mode);

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = make_request(req, 0);
  assert(reply);
  dout(3) << "open result = " << reply->get_result() << endl;

  vector<c_inode_info*> trace = reply->get_trace();
  this->insert_trace(trace);  
  int result = reply->get_result();

  // success?
  if (result > 0) {
	// yay
	Fh *f = new Fh;
	memset(f, 0, sizeof(*f));
	f->ino = trace[trace.size()-1]->inode.ino;
	f->mds = reply->get_source();
	f->caps = reply->get_file_caps();
	fh_map[reply->get_result()] = f;

	dout(3) << "open success, fh is " << reply->get_result() << " caps " << f->caps << "  fh size " << f->size << endl;
  }

  delete reply;

  client_lock.Unlock();
  return result;
}

int Client::close(fileh_t fh)
{
  client_lock.Lock();

  dout(3) << "close " << fh << endl;
  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];

  MClientRequest *req = new MClientRequest(MDS_OP_CLOSE, whoami);
  req->set_iarg(fh);
  req->set_ino(f->ino);

  req->set_targ( f->mtime );
  req->set_sizearg( f->size );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  // take note of the fact that we're mid-close
  fh_closing.insert(fh);

  MClientReply *reply = make_request(req, 0);
  assert(reply);
  int result = reply->get_result();
  dout(3) << "close " << fh << " result = " << result << endl;
  
  fh_closing.erase(fh);
  fh_map.erase(fh);
  
  delete reply;

  client_lock.Unlock();
  return result;
}



// ------------
// read, write

// ------------------------
// blocking osd interface

class C_Client_Cond : public Context {
public:
  Cond *cond;
  Mutex *mutex;
  int *rvalue;
  C_Client_Cond(Cond *cond, Mutex *mutex, int *rvalue) { 
	this->cond = cond;
	this->mutex = mutex;
	this->rvalue = rvalue;
  }
  void finish(int r) {
	*rvalue = r;
	cond->Signal();
  }
};


int Client::read(fileh_t fh, char *buf, size_t size, off_t offset) 
{
  client_lock.Lock();

  assert(fh_map.count(fh));
  inodeno_t ino = fh_map[fh]->ino;
  
  // check current file mode (are we allowed to read, cache, etc.)
  // ***
  
  bufferlist blist;   // data will go here

  // issue async read
  Cond cond;
  int rvalue;

  C_Client_Cond *onfinish = new C_Client_Cond(&cond, &client_lock, &rvalue);
  filer->read(ino, size, offset, &blist, onfinish);
  
  cond.Wait(client_lock);

  // copy data into caller's buf
  blist.copy(0, blist.length(), buf);

  client_lock.Unlock();
  return rvalue;  
}



int Client::write(fileh_t fh, const char *buf, size_t size, off_t offset) 
{
  client_lock.Lock();

  dout(7) << "write fh " << fh << " size " << size << " offset " << offset << endl;

  assert(fh_map.count(fh));
  inodeno_t ino = fh_map[fh]->ino;

  Fh *f = fh_map[fh];
  Inode *in = inode_map[ino];

  dout(10) << "cur file size is " << in->inode.size << "    fh size " << f->size << endl;

  // check current file mode (are we allowed to write, buffer, etc.)
  // ***
  
  // create a buffer that refers to *buf, but doesn't try to free it when it's done.
  bufferlist blist;
  blist.push_back( new buffer(buf, size, BUFFER_MODE_NOCOPY|BUFFER_MODE_NOFREE) );

  // issue write
  Cond cond;
  int rvalue;

  C_Client_Cond *onfinish = new C_Client_Cond(&cond, &client_lock, &rvalue);
  filer->write(ino, size, offset, blist, 0, onfinish);
  
  cond.Wait(client_lock);

  // assume success for now.  FIXME.
  size_t totalwritten = size;
  
  // extend file?
  if (totalwritten + offset > f->size) {
	f->size = totalwritten + offset;
	in->inode.size = f->size;
	dout(7) << "wrote to " << totalwritten+offset << ", extending file size" << endl;
  } else {
	dout(7) << "wrote to " << totalwritten+offset << ", leaving file size at " << f->size << endl;
  }

  // mtime
  f->mtime = in->inode.mtime = g_clock.gettime();

  // ok!
  client_lock.Unlock();
  return totalwritten;  
}



// not written yet, but i want to link!

int Client::statfs(const char *path, struct statfs *stbuf) {}
