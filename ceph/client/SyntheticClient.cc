
#include "SyntheticClient.h"

#include "include/filepath.h"
#include "mds/MDS.h"


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <utime.h>
#include <math.h>

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_client) cout << "synthetic" << client->get_nodeid() << " "

// traces
//void trace_include(SyntheticClient *syn, Client *cl, string& prefix);
//void trace_openssh(SyntheticClient *syn, Client *cl, string& prefix);


#define DBL 2

void *synthetic_client_thread_entry(void *ptr)
{
  SyntheticClient *sc = (SyntheticClient*)ptr;
  int r = sc->run();
  return (void*)r;
}

string SyntheticClient::get_sarg() 
{
  string a;
  if (!sargs.empty()) {
	a = sargs.front(); 
	sargs.pop_front();
  }
  if (a.length() == 0 || a == "~") {
	char s[20];
	sprintf(s,"syn.%d", client->whoami);
	a = s;
  } 
  //cout << "a is " << a << endl;
  return a;
}

int SyntheticClient::run()
{ 
  run_start = g_clock.gettimepair();
  run_until = timepair_t(0,0);

  for (list<int>::iterator it = modes.begin();
	   it != modes.end();
	   it++) {
	int mode = *it;
		 
	switch (mode) {
	case SYNCLIENT_MODE_RANDOMSLEEP:
	  {
		int iarg1 = iargs.front();
		iargs.pop_front();
		srand(time(0) + getpid());
		sleep(rand() % iarg1);
	  }
	  break;

	case SYNCLIENT_MODE_UNTIL:
	  {
		int iarg1 = iargs.front();
		iargs.pop_front();
		if (iarg1) {
		  dout(2) << "until " << iarg1 << endl;
		  timepair_t dur(iarg1,0);
		  run_until = run_start + dur;
		} else {
		  dout(2) << "until " << iarg1 << " (no limit)" << endl;
		  run_until = timepair_t(0,0);
		}
	  }
	  break;

	case SYNCLIENT_MODE_RANDOMWALK:
	  {
		int iarg1 = iargs.front();
		iargs.pop_front();
		dout(2) << "randomwalk " << iarg1 << endl;
		random_walk(iarg1);
	  }
	  break;

	case SYNCLIENT_MODE_MAKEDIRS:
	  {
		string sarg1 = get_sarg();
		int iarg1 = iargs.front();  iargs.pop_front();
		int iarg2 = iargs.front();  iargs.pop_front();
		int iarg3 = iargs.front();  iargs.pop_front();
		dout(2) << "makedirs " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << endl;
		make_dirs(sarg1.c_str(), iarg1, iarg2, iarg3);
	  }
	  break;

	case SYNCLIENT_MODE_FULLWALK:
	  {
		string sarg1 = get_sarg();
		dout(2) << "fullwalk" << sarg1 << endl;
		full_walk(sarg1);
	  }
	  break;
	case SYNCLIENT_MODE_REPEATWALK:
	  {
		string sarg1 = get_sarg();
		dout(2) << "repeatwalk " << sarg1 << endl;
		while (full_walk(sarg1) == 0) ;
	  }
	  break;

	case SYNCLIENT_MODE_WRITEFILE:
	  {
		string sarg1 = get_sarg();
		int iarg1 = iargs.front();  iargs.pop_front();
		int iarg2 = iargs.front();  iargs.pop_front();
		write_file(sarg1, iarg1, iarg2);
	  }
	  break;
	case SYNCLIENT_MODE_READFILE:
	  {
		string sarg1 = get_sarg();
		int iarg1 = iargs.front();  iargs.pop_front();
		int iarg2 = iargs.front();  iargs.pop_front();
		read_file(sarg1, iarg1, iarg2);
	  }
	  break;


	case SYNCLIENT_MODE_TRACEINCLUDE:
	  {
		int iarg1 = iargs.front();  iargs.pop_front();
		string prefix;
		if (client->whoami == 0) {
		  Trace t("traces/trace.include");
		  play_trace(t, prefix);
		} else {
		  sleep(iarg1);
		}
	  }
	  break;
	case SYNCLIENT_MODE_TRACELIB:
	  {
		int iarg1 = iargs.front();  iargs.pop_front();
		string prefix;
		if (client->whoami == 0) {
		  Trace t("traces/trace.lib");
		  play_trace(t, prefix);
		} else {
		  sleep(iarg1);
		}
	  }
	  break;
	case SYNCLIENT_MODE_TRACEOPENSSH:
	  {
		string prefix = get_sarg();
		int iarg1 = iargs.front();  iargs.pop_front();
		
		Trace t("traces/trace.openssh");

		client->mkdir(prefix.c_str(), 0755);
		
		for (int i=0; i<iarg1; i++) {
		  if (time_to_stop()) break;
		  play_trace(t, prefix);
		  if (time_to_stop()) break;
		  clean_dir(prefix);
		}
	  }
	  break;
	case SYNCLIENT_MODE_TRACEOPENSSHLIB:
	  {
		string prefix = get_sarg();
		int iarg1 = iargs.front();  iargs.pop_front();
		
		Trace t("traces/trace.openssh.lib");

		client->mkdir(prefix.c_str(), 0755);
		
		for (int i=0; i<iarg1; i++) {
		  if (time_to_stop()) break;
		  play_trace(t, prefix);
		  if (time_to_stop()) break;
		  clean_dir(prefix);
		}
	  }
	  break;

	default:
	  assert(0);
	}
  }
  return 0;
}


int SyntheticClient::start_thread()
{
  assert(!thread_id);

  pthread_create(&thread_id, NULL, synthetic_client_thread_entry, this);
  assert(thread_id);
}

int SyntheticClient::join_thread()
{
  assert(thread_id);
  void *rv;
  pthread_join(thread_id, &rv);
}


bool roll_die(float p) 
{
  float r = (float)(rand() % 100000) / 100000.0;
  if (r < p) 
	return true;
  else 
	return false;
}

void SyntheticClient::init_op_dist()
{
  op_dist.clear();
  op_dist.add( MDS_OP_STAT, g_conf.fakeclient_op_stat );
  op_dist.add( MDS_OP_UTIME, g_conf.fakeclient_op_utime );
  op_dist.add( MDS_OP_CHMOD, g_conf.fakeclient_op_chmod );
  op_dist.add( MDS_OP_CHOWN, g_conf.fakeclient_op_chown );

  op_dist.add( MDS_OP_READDIR, g_conf.fakeclient_op_readdir );
  op_dist.add( MDS_OP_MKNOD, g_conf.fakeclient_op_mknod );
  op_dist.add( MDS_OP_LINK, g_conf.fakeclient_op_link );
  op_dist.add( MDS_OP_UNLINK, g_conf.fakeclient_op_unlink );
  op_dist.add( MDS_OP_RENAME, g_conf.fakeclient_op_rename );

  op_dist.add( MDS_OP_MKDIR, g_conf.fakeclient_op_mkdir );
  op_dist.add( MDS_OP_RMDIR, g_conf.fakeclient_op_rmdir );
  op_dist.add( MDS_OP_SYMLINK, g_conf.fakeclient_op_symlink );

  op_dist.add( MDS_OP_OPEN, g_conf.fakeclient_op_openrd );
  //op_dist.add( MDS_OP_READ, g_conf.fakeclient_op_read );
  //op_dist.add( MDS_OP_WRITE, g_conf.fakeclient_op_write );
  op_dist.add( MDS_OP_TRUNCATE, g_conf.fakeclient_op_truncate );
  op_dist.add( MDS_OP_FSYNC, g_conf.fakeclient_op_fsync );
  op_dist.add( MDS_OP_CLOSE, g_conf.fakeclient_op_close );
  op_dist.normalize();
}

void SyntheticClient::up()
{
  cwd = cwd.prefixpath(cwd.depth()-1);
  dout(DBL) << "cd .. -> " << cwd << endl;
  clear_dir();
}


int SyntheticClient::play_trace(Trace& t, string& prefix)
{
  dout(4) << "play trace" << endl;
  t.start();

  const char *p = prefix.c_str();

  map<__int64_t, __int64_t> open_files;

  while (!t.end()) {
	
	if (time_to_stop()) break;
	
	// op
	const char *op = t.get_string();
	dout(4) << "trace op " << op << endl;
	if (strcmp(op, "link") == 0) {
	  const char *a = t.get_string(p);
	  const char *b = t.get_string(p);
	  client->link(a,b);	  
	} else if (strcmp(op, "unlink") == 0) {
	  const char *a = t.get_string(p);
	  client->unlink(a);
	} else if (strcmp(op, "rename") == 0) {
	  const char *a = t.get_string(p);
	  const char *b = t.get_string(p);
	  client->rename(a,b);	  
	} else if (strcmp(op, "mkdir") == 0) {
	  const char *a = t.get_string(p);
	  __int64_t b = t.get_int();
	  client->mkdir(a, b);
	} else if (strcmp(op, "rmdir") == 0) {
	  const char *a = t.get_string(p);
	  client->rmdir(a);
	} else if (strcmp(op, "symlink") == 0) {
	  const char *a = t.get_string(p);
	  const char *b = t.get_string(p);
	  client->symlink(a,b);	  
	} else if (strcmp(op, "readlink") == 0) {
	  const char *a = t.get_string(p);
	  char buf[100];
	  client->readlink(a, buf, 100);
	} else if (strcmp(op, "lstat") == 0) {
	  struct stat st;
	  const char *a = t.get_string(p);
	  client->lstat(a, &st);
	} else if (strcmp(op, "chmod") == 0) {
	  const char *a = t.get_string(p);
	  __int64_t b = t.get_int();
	  client->chmod(a, b);
	} else if (strcmp(op, "chown") == 0) {
	  const char *a = t.get_string(p);
	  __int64_t b = t.get_int();
	  __int64_t c = t.get_int();
	  client->chown(a, b, c);
	} else if (strcmp(op, "utime") == 0) {
	  const char *a = t.get_string(p);
	  __int64_t b = t.get_int();
	  __int64_t c = t.get_int();
	  struct utimbuf u;
	  u.actime = b;
	  u.modtime = c;
	  client->utime(a, &u);
	} else if (strcmp(op, "mknod") == 0) {
	  const char *a = t.get_string(p);
	  __int64_t b = t.get_int();
	  client->mknod(a, b);
	} else if (strcmp(op, "getdir") == 0) {
	  const char *a = t.get_string(p);
	  map<string,inode_t*> contents;
	  client->getdir(a, contents);
	} else if (strcmp(op, "open") == 0) {
	  const char *a = t.get_string(p);
	  __int64_t b = t.get_int(); 
	  // HACK
	  b = O_RDONLY;
	  __int64_t id = t.get_int();
	  __int64_t fh = client->open(a, b);
	  open_files[id] = fh;
	} else if (strcmp(op, "close") == 0) {
	  __int64_t id = t.get_int();
	  __int64_t fh = open_files[id];
	  if (fh > 0) client->close(fh);
	  open_files.erase(id);
	} else if (strcmp(op, "truncate") == 0) {
	  const char *a = t.get_string(p);
	  __int64_t b = t.get_int();
	  client->truncate(a,b);
	} else if (strcmp(op, "fsync") == 0) {
	  assert(0);
	} else 
	  assert(0);
  }

  // close open files
  for (map<__int64_t, __int64_t>::iterator fi = open_files.begin();
	   fi != open_files.end();
	   fi++) {
	dout(1) << "leftover close " << fi->second << endl;
	client->close(fi->second);
  }
  
}


int SyntheticClient::clean_dir(string& basedir)
{
  // read dir
  map<string, inode_t*> contents;
  int r = client->getdir(basedir.c_str(), contents);
  if (r < 0) {
	dout(1) << "readdir on " << basedir << " returns " << r << endl;
	return r;
  }

  for (map<string, inode_t*>::iterator it = contents.begin();
	   it != contents.end();
	   it++) {
	string file = basedir + "/" + it->first;

	if (time_to_stop()) break;

	struct stat st;
	int r = client->lstat(file.c_str(), &st);
	if (r < 0) {
	  dout(1) << "stat error on " << file << " r=" << r << endl;
	  continue;
	}

	if (st.st_mode & INODE_MODE_DIR) {
	  clean_dir(file);
	  client->rmdir(file.c_str());
	} else {
	  client->unlink(file.c_str());
	}
  }

  return 0;

}


int SyntheticClient::full_walk(string& basedir) 
{
  if (time_to_stop()) return -1;

  // read dir
  map<string, inode_t*> contents;
  int r = client->getdir(basedir.c_str(), contents);
  if (r < 0) {
	dout(1) << "readdir on " << basedir << " returns " << r << endl;
	return r;
  }

  for (map<string, inode_t*>::iterator it = contents.begin();
	   it != contents.end();
	   it++) {
	string file = basedir + "/" + it->first;

	struct stat st;
	int r = client->lstat(file.c_str(), &st);
	if (r < 0) {
	  dout(1) << "stat error on " << file << " r=" << r << endl;
	  continue;
	}

	if (st.st_mode & INODE_MODE_DIR) full_walk(file);
  }

  return 0;
}

int SyntheticClient::make_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  // make sure base dir exists
  int r = client->mkdir(basedir, 0755);
  if (r != 0) {
	dout(1) << "can't make base dir? " << basedir << endl;
	return -1;
  }

  // children
  char d[500];
  dout(5-depth) << "make_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << endl;
  for (int i=0; i<files; i++) {
	sprintf(d,"%s/file.%d", basedir, i);
	client->mknod(d, 0644);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
	sprintf(d, "%s/dir.%d", basedir, i);
	make_dirs(d, dirs, files, depth-1);
  }
  
  return 0;
}



int SyntheticClient::write_file(string& fn, int size, int wrsize)   // size is in MB, wrsize in bytes
{
  //__uint64_t wrsize = 1024*256;
  char *buf = new char[wrsize];   // 1 MB
  memset(buf, 1, wrsize);
  __uint64_t chunks = (__uint64_t)size * (__uint64_t)(1024*1024) / (__uint64_t)wrsize;

  int fd = client->open(fn.c_str(), O_WRONLY|O_CREAT);
  dout(5) << "writing to " << fn << " fd " << fd << endl;
  if (fd < 0) return fd;

  for (int i=0; i<chunks; i++) {
	if (time_to_stop()) break;
	dout(2) << "writing block " << i << "/" << chunks << endl;
	client->write(fd, buf, wrsize, i*wrsize);
  }
  
  client->close(fd);
  delete[] buf;
}

int SyntheticClient::read_file(string& fn, int size, int rdsize)   // size is in MB, wrsize in bytes
{
  char *buf = new char[rdsize]; 
  memset(buf, 1, rdsize);
  __uint64_t chunks = (__uint64_t)size * (__uint64_t)(1024*1024) / (__uint64_t)rdsize;

  int fd = client->open(fn.c_str(), O_RDONLY);
  dout(5) << "reading from " << fn << " fd " << fd << endl;
  if (fd < 0) return fd;

  for (int i=0; i<chunks; i++) {
	if (time_to_stop()) break;
	dout(2) << "reading block " << i << "/" << chunks << endl;
	client->read(fd, buf, rdsize, i*rdsize);
  }
  
  client->close(fd);
  delete[] buf;
}



int SyntheticClient::random_walk(int num_req)
{
  int left = num_req;

  //dout(1) << "random_walk() will do " << left << " ops" << endl;

  init_op_dist();  // set up metadata op distribution
 
  while (left > 0) {
	left--;

	if (time_to_stop()) break;

	// ascend?
	if (cwd.depth() && !roll_die(pow(.9, cwd.depth()))) {
	  dout(DBL) << "die says up" << endl;
	  up();
	  continue;
	}

	// descend?
	if (.9*roll_die(pow(.9,cwd.depth())) && subdirs.size()) {
	  string s = get_random_subdir();
	  cwd.add_dentry( s );
	  dout(DBL) << "cd " << s << " -> " << cwd << endl;
	  clear_dir();
	  continue;
	}

	int op = 0;
	filepath path;

	if (contents.empty() && roll_die(.3)) {
	  if (did_readdir) {
		dout(DBL) << "empty dir, up" << endl;
		up();
	  } else
		op = MDS_OP_READDIR;
	} else {
	  op = op_dist.sample();
	}
	//dout(DBL) << "op is " << op << endl;

	int r = 0;

	// do op
	if (op == MDS_OP_UNLINK) {
	  if (contents.empty())
		op = MDS_OP_READDIR;
	  else 
		r = client->unlink( get_random_sub() );   // will fail on dirs
	}
	 
	if (op == MDS_OP_RENAME) {
	  if (contents.empty())
		op = MDS_OP_READDIR;
	  else {
		r = client->rename( get_random_sub(), make_sub("ren") );
	  }
	}
	
	if (op == MDS_OP_MKDIR) {
	  r = client->mkdir( make_sub("mkdir"), 0755);
	}
	
	if (op == MDS_OP_RMDIR) {
	  if (!subdirs.empty())
		r = client->rmdir( get_random_subdir() );
	  else
		r = client->rmdir( cwd.c_str() );     // will pbly fail
	}
	
	if (op == MDS_OP_SYMLINK) {
	}
	
	if (op == MDS_OP_CHMOD) {
	  if (contents.empty())
		op = MDS_OP_READDIR;
	  else
		r = client->chmod( get_random_sub(), rand() & 0755 );
	}
	
	if (op == MDS_OP_CHOWN) {
	  if (contents.empty()) 		r = client->chown( cwd.c_str(), rand(), rand() );
	  else
		r = client->chown( get_random_sub(), rand(), rand() );
	}
	 
	if (op == MDS_OP_LINK) {
	}
	 
	if (op == MDS_OP_UTIME) {
	  struct utimbuf b;
	  memset(&b, 1, sizeof(b));
	  if (contents.empty()) 
		r = client->utime( cwd.c_str(), &b );
	  else
		r = client->utime( get_random_sub(), &b );
	}
	
	if (op == MDS_OP_MKNOD) {
	  r = client->mknod( make_sub("mknod"), 0644);
	}
	 
	if (op == MDS_OP_OPEN) {
	  if (contents.empty())
		op = MDS_OP_READDIR;
	  else {
		r = client->open( get_random_sub(), O_RDONLY );
		if (r > 0) {
		  assert(open_files.count(r) == 0);
		  open_files.insert(r);
		}
	  }
	}

	if (op == MDS_OP_CLOSE) {
	  if (open_files.empty())
		op = MDS_OP_STAT;
	  else {
		int fh = get_random_fh();
		r = client->close( fh );
		if (r == 0) open_files.erase(fh);
	  }
	}
	
	if (op == MDS_OP_STAT) {
	  struct stat st;
	  if (contents.empty()) {
		if (did_readdir) {
		  if (roll_die(.1)) {
			dout(DBL) << "stat in empty dir, up" << endl;
			up();
		  } else {
			op = MDS_OP_MKNOD;
		  }
		} else
		  op = MDS_OP_READDIR;
	  } else
		r = client->lstat(get_random_sub(), &st);
	}

	if (op == MDS_OP_READDIR) {
	  clear_dir();
	  
	  map<string, inode_t*> c;
	  r = client->getdir( cwd.c_str(), c );
	  
	  for (map<string, inode_t*>::iterator it = c.begin();
		   it != c.end();
		   it++) {
		//dout(DBL) << " got " << it->first << endl;
		contents[it->first] = *(it->second);
		if (it->second->mode & INODE_MODE_DIR) 
		  subdirs.insert(it->first);
	  }
	  
	  did_readdir = true;
	}
	  
	// errors?
	if (r < 0) {
	  // reevaluate cwd.
	  //while (cwd.depth()) {
	  //if (client->lookup(cwd)) break;   // it's in the cache
		
	  //dout(DBL) << "r = " << r << ", client doesn't have " << cwd << ", cd .." << endl;
	  dout(DBL) << "r = " << r << ", client may not have " << cwd << ", cd .." << endl;
	  up();
	  //}	  
	}
  }

  // close files
  dout(DBL) << "closing files" << endl;
  while (!open_files.empty()) {
	int fh = get_random_fh();
	int r = client->close( fh );
	if (r == 0) open_files.erase(fh);
  }

  dout(DBL) << "done" << endl;
  return 0;
}


