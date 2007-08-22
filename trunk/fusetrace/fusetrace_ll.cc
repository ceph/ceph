// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:t -*- 
// vim: ts=8 sw=4 smarttab

/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.

    gcc -Wall `pkg-config fuse --cflags --libs` -lulockmgr fusexmp_fh.c -o fusexmp_fh
*/

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif


#include <fuse/fuse_lowlevel.h>
#include <ulockmgr.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif


#include <ext/hash_map>
using namespace __gnu_cxx;

#ifndef __LP64__
namespace __gnu_cxx {
  template<> struct hash<uint64_t> {
    size_t operator()(uint64_t __x) const { 
      static hash<uint32_t> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
}
#endif


#include <iostream>
#include <fstream>
#include <map>
using namespace std;

#include "common/Mutex.h"

Mutex trace_lock;
ofstream tracefile;

#define traceout (tracefile.is_open() ? tracefile : cout)

char *basedir = 0;

Mutex lock;
struct Inode;
struct Dir {
    Inode *inode;
    map<string,Inode*> dentries;
    Dir(Inode *p) : inode(p) {}
};
struct Inode {
    struct stat stbuf;
    Dir *parent_dir;
    string parent_dn;
    Dir *dir;
    int ref;
    Inode() : parent_dir(0), dir(0), ref(0) {}
};

hash_map<ino_t, Inode*> inode_map;

void make_inode_path(string &buf, Inode *in)
{
    if (in->parent_dir) 
	make_inode_path(buf, in->parent_dir->inode);
    else
	buf = basedir;
    buf += "/";
    buf += in->parent_dn;
    //cout << "path: " << in->stbuf.st_ino << " -> " << buf << endl;
}

void make_inode_path(string &buf, Inode *in, const char *name)
{
    make_inode_path(buf, in);
    buf += "/";
    buf += name;
}

void make_ino_path(string &buf, ino_t ino)
{
    Inode *in = inode_map[ino];
    assert(in);
    make_inode_path(buf, in);
}

void make_ino_path(string &buf, ino_t ino, const char *name)
{
    Inode *in = inode_map[ino];
    assert(in);
    make_inode_path(buf, in);
    buf += "/";
    buf += name;
}

void remove_inode(Inode *in)
{
    //cout << "remove_inode " << in->stbuf.st_ino << endl;
    if (in->parent_dir)
	in->parent_dir->dentries.erase(in->parent_dn);
    inode_map.erase(in->stbuf.st_ino);
    if (in->dir) {
	while (!in->dir->dentries.empty()) 
	    remove_inode(in->dir->dentries.begin()->second);
	delete in->dir;
    }
    delete in;
}

Inode *add_inode(Inode *parent, const char *name, struct stat *attr)
{
    Inode *in = new Inode;
    memcpy(&in->stbuf, attr, sizeof(*attr));

    if (!parent->dir) parent->dir = new Dir(parent);

    string dname(name);
    parent->dir->dentries[dname] = in;
    inode_map[in->stbuf.st_ino] = in;

    in->parent_dn = dname;
    in->parent_dir = parent->dir;

    return in;
}



static void ft_ll_lookup(fuse_req_t req, fuse_ino_t pino, const char *name)
{
    int res = 0;

    //cout << "lookup " << pino << " " << name << endl;

    struct fuse_entry_param fe;
    memset(&fe, 0, sizeof(fe));

    lock.Lock();
    Inode *parent = inode_map[pino];
    assert(parent);
    if (!parent->dir) parent->dir = new Dir(parent);
    parent->ref++;

    string dname(name);
    Inode *in = 0;
    if (parent->dir->dentries.count(dname)) {
	in = parent->dir->dentries[dname];
	//cout << "have " << in->stbuf.st_ino << endl;
    } else {
	string path;
	make_inode_path(path, parent, name);
	in = new Inode;
	res = lstat(path.c_str(), &in->stbuf);
	//cout << "stat " << path << " res = " << res << endl;
	if (res == 0) {
	    parent->dir->dentries[dname] = in;
	    inode_map[in->stbuf.st_ino] = in;
	    in->parent_dn = dname;
	    in->parent_dir = parent->dir;
	} else {
	    delete in;
	    in = 0;
	    res = errno;
	}
    }
    if (in) {
	fe.ino = in->stbuf.st_ino;
	memcpy(&fe.attr, &in->stbuf, sizeof(in->stbuf));
    }
    lock.Unlock();

    trace_lock.Lock();
    traceout << "ll_lookup" << endl << pino << endl << name << endl << fe.attr.st_ino << endl;
    trace_lock.Unlock();

    if (in) 
	fuse_reply_entry(req, &fe);
    else 
	fuse_reply_err(req, res);
}

static void ft_ll_forget(fuse_req_t req, fuse_ino_t ino, long unsigned nlookup)
{
    if (ino != 1) {
	lock.Lock();
	Inode *in = inode_map[ino];
	if (in) {
	    in->ref -= nlookup;
	    if (in->ref == 0) 
		remove_inode(in);
	} else {
	    //cout << "weird, don't have inode " << ino << endl;
	}
	lock.Unlock();
    }

    trace_lock.Lock();
    traceout << "ll_forget" << endl << ino << endl << nlookup << endl;
    trace_lock.Unlock();

    fuse_reply_none(req);
}

static void ft_ll_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    int res;
    string path;

    Inode *in = 0;
    struct stat attr;

    lock.Lock();
    in = inode_map[ino];
    make_inode_path(path, in);
    lock.Unlock();

    res = ::lstat(path.c_str(), &attr);
    //cout << "getattr stat on " << path << " res " << res << endl;
    if (ino == 1)
	in->stbuf.st_ino = 1;
    
    trace_lock.Lock();
    traceout << "ll_getattr" << endl << ino << endl;
    trace_lock.Unlock();

    if (res == 0) {
	lock.Lock();
	memcpy(&in->stbuf, &attr, sizeof(attr));
	lock.Unlock();
	fuse_reply_attr(req, &attr, 0);
    } else 
	fuse_reply_err(req, -errno);
}

static void ft_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			  int to_set, struct fuse_file_info *fi)
{
    string path;
    Inode *in = 0;
    lock.Lock();
    in = inode_map[ino];
    make_inode_path(path, in);
    lock.Unlock();

    trace_lock.Lock();
    traceout << "ll_setattr" << endl << ino << endl;
    traceout << attr->st_mode << endl;
    traceout << attr->st_uid << endl << attr->st_gid << endl;
    traceout << attr->st_size << endl;
    traceout << attr->st_mtime << endl;
    traceout << attr->st_atime << endl;
    traceout << to_set << endl;
    trace_lock.Unlock();

    int res = 0;
    if (to_set & FUSE_SET_ATTR_MODE)
	res = ::chmod(path.c_str(), attr->st_mode);
    if (!res && to_set & FUSE_SET_ATTR_UID)
	res = ::chown(path.c_str(), attr->st_uid, attr->st_gid);
    if (!res && to_set & FUSE_SET_ATTR_SIZE)
	res = ::truncate(path.c_str(), attr->st_size);
    if (!res && to_set & FUSE_SET_ATTR_MTIME) {
	struct utimbuf ut;
	ut.actime = attr->st_atime;
	ut.modtime = attr->st_mtime;
	res = ::utime(path.c_str(), &ut);
    }

    if (res == 0) {
	lock.Lock();
	::lstat(path.c_str(), &in->stbuf);
	memcpy(attr, &in->stbuf, sizeof(*attr));
	lock.Unlock();
	fuse_reply_attr(req, attr, 0);
    } else
	fuse_reply_err(req, errno);    
}


static void ft_ll_readlink(fuse_req_t req, fuse_ino_t ino)
{
    string path;
    lock.Lock();
    make_ino_path(path, ino);
    lock.Unlock();

    trace_lock.Lock();
    traceout << "ll_readlink" << endl << ino << endl;
    trace_lock.Unlock();

    char buf[256];
    int res = readlink(path.c_str(), buf, 255);
    if (res >= 0) {
	buf[res] = 0;
	fuse_reply_readlink(req, buf);
    } else {
	fuse_reply_err(req, errno);
    }
}


static void ft_ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    string path;
    lock.Lock();
    make_ino_path(path, ino);
    lock.Unlock();
    
    DIR *dir = opendir(path.c_str());

    trace_lock.Lock();
    traceout << "ll_opendir" << endl << ino << endl << (unsigned long)dir << endl;
    trace_lock.Unlock();
    
    if (dir) {
	fi->fh = (long)dir;
	fuse_reply_open(req, fi);
    } else 
	fuse_reply_err(req, errno);
}

static void ft_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
			    off_t off, struct fuse_file_info *fi)
{
    struct dirent *de;
    DIR *dp = (DIR*)fi->fh;

    // buffer
    char *buf;
    size_t pos = 0;
    
    buf = new char[size];
    if (!buf) {
	fuse_reply_err(req, ENOMEM);
	return;
    }

    seekdir(dp, off);
    while ((de = readdir(dp)) != NULL) {
	struct stat st;
	memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
	
	size_t entrysize = fuse_add_direntry(req, buf + pos, size - pos,
					     de->d_name, &st, telldir(dp));
	if (entrysize > size - pos) 
	    break;  // didn't fit, done for now.
	pos += entrysize;
    }

    fuse_reply_buf(req, buf, pos);
    delete[] buf;
}

static void ft_ll_releasedir(fuse_req_t req, fuse_ino_t ino,
			     struct fuse_file_info *fi)
{
  DIR *dir = (DIR*)fi->fh;

  trace_lock.Lock();
  traceout << "ll_releasedir" << endl << (unsigned long)dir << endl;
  trace_lock.Unlock();

  closedir(dir);
  fuse_reply_err(req, 0);
}



static void ft_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
			mode_t mode, dev_t rdev)
{
    string path;
    Inode *pin = 0;
    lock.Lock();
    pin = inode_map[parent];
    make_inode_path(path, pin, name);
    lock.Unlock();

    int res = ::mknod(path.c_str(), mode, rdev);

    struct fuse_entry_param fe;
    if (res == 0) {
	memset(&fe, 0, sizeof(fe));
	::lstat(path.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
	lock.Lock();
	add_inode(pin, name, &fe.attr);
	lock.Unlock();
    }

    trace_lock.Lock();
    traceout << "ll_mknod" << endl << parent << endl << name << endl << mode << endl << rdev << endl;
    traceout << (res == 0 ? fe.ino:0) << endl;
    trace_lock.Unlock();

    if (res == 0)
	fuse_reply_entry(req, &fe);
    else 
	fuse_reply_err(req, errno);
}

static void ft_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
			mode_t mode)
{
    string path;
    Inode *pin = 0;
    lock.Lock();
    pin = inode_map[parent];
    make_inode_path(path, pin, name);
    lock.Unlock();

    int res = ::mkdir(path.c_str(), mode);

    struct fuse_entry_param fe;
    if (res == 0) {
	memset(&fe, 0, sizeof(fe));
	::lstat(path.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
  	lock.Lock();
	add_inode(pin, name, &fe.attr);
  	lock.Unlock();
    }

    trace_lock.Lock();
    traceout << "ll_mkdir" << endl << parent << endl << name << endl << mode << endl;
    traceout << (res == 0 ? fe.ino:0) << endl;
    trace_lock.Unlock();

    if (res == 0)
	fuse_reply_entry(req, &fe);
    else 
	fuse_reply_err(req, errno);
}

static void ft_ll_symlink(fuse_req_t req, const char *value, fuse_ino_t parent, const char *name)
{
    string path;
    Inode *pin = 0;
    lock.Lock();
    pin = inode_map[parent];
    make_inode_path(path, pin, name);
    lock.Unlock();

    int res = ::symlink(value, path.c_str());

    struct fuse_entry_param fe;
    if (res == 0) {
	memset(&fe, 0, sizeof(fe));
	::lstat(path.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
      	lock.Lock();
	add_inode(pin, name, &fe.attr);
    	lock.Unlock();
    }

    trace_lock.Lock();
    traceout << "ll_symlink" << endl << parent << endl << name << endl << value << endl;
    traceout << (res == 0 ? fe.ino:0) << endl;
    trace_lock.Unlock();

    if (res == 0)
	fuse_reply_entry(req, &fe);
    else 
	fuse_reply_err(req, errno);
}

static void ft_ll_statfs(fuse_req_t req, fuse_ino_t ino)
{
    string path;
    if (ino) {
	lock.Lock();
	make_ino_path(path, ino);
	lock.Unlock();
    } else {
	path = basedir;
    }

    trace_lock.Lock();
    traceout << "ll_statfs" << endl << ino << endl;
    trace_lock.Unlock();
    
    struct statvfs stbuf;
    int res = statvfs(path.c_str(), &stbuf);
    if (res == 0) 
	fuse_reply_statfs(req, &stbuf);
    else 
	fuse_reply_err(req, errno);
}

static void ft_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    string path;
    Inode *pin = 0;
    lock.Lock();
    pin = inode_map[parent];
    make_inode_path(path, pin, name);
    lock.Unlock();

    trace_lock.Lock();
    traceout << "ll_unlink" << endl << parent << endl << name << endl;
    trace_lock.Unlock();

    int res = ::unlink(path.c_str());

    if (res == 0) {
	// remove from out cache
  	lock.Lock();
	string dname(name);
	if (pin->dir &&
	    pin->dir->dentries.count(dname))
	    remove_inode(pin->dir->dentries[dname]);
	lock.Unlock();
	fuse_reply_err(req, 0);
    } else
	fuse_reply_err(req, errno);
}

static void ft_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    string path;
    Inode *pin = 0;
    lock.Lock();
    pin = inode_map[parent];
    make_inode_path(path, pin, name);
    lock.Unlock();

    trace_lock.Lock();
    traceout << "ll_rmdir" << endl << parent << endl << name << endl;
    trace_lock.Unlock();

    int res = ::rmdir(path.c_str());

    if (res == 0) {
	// remove from out cache
	lock.Lock();
	string dname(name);
	if (pin->dir &&
	    pin->dir->dentries.count(dname))
	    remove_inode(pin->dir->dentries[dname]);
	lock.Unlock();
	fuse_reply_err(req, 0);
    } else
	fuse_reply_err(req, errno);
}


static void ft_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
			 fuse_ino_t newparent, const char *newname)
{
    string path;
    string newpath;
    Inode *pin = 0;
    Inode *newpin = 0;
    lock.Lock();
    pin = inode_map[parent];
    make_inode_path(path, pin, name);
    newpin = inode_map[newparent];
    make_inode_path(newpath, newpin, newname);
    lock.Unlock();

    trace_lock.Lock();
    traceout << "ll_rename" << endl
	     << parent << endl 
	     << name << endl
	     << newparent << endl
	     << newname << endl;
    trace_lock.Unlock();

    int res = ::rename(path.c_str(), newpath.c_str());
    
    if (res == 0) {
	string dname(name);
	string newdname(newname);
	lock.Lock();
	if (pin->dir &&
	    pin->dir->dentries.count(dname)) {
	    Inode *in = pin->dir->dentries[dname];
	    pin->dir->dentries.erase(dname);
	    
	    if (!newpin->dir) newpin->dir = new Dir(newpin);
	    newpin->dir->dentries[newdname] = in;
	    in->parent_dir = newpin->dir;
	    in->parent_dn = newdname;
	}
	lock.Unlock();
	fuse_reply_err(req, 0);
    } else 
	fuse_reply_err(req, errno);
}

static void ft_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
		       const char *newname)
{
    string path;
    string newpath;
    Inode *newpin = 0;
    lock.Lock();
    make_ino_path(path, ino);
    newpin = inode_map[newparent];
    make_inode_path(newpath, newpin, newname);
    lock.Unlock();

    trace_lock.Lock();
    traceout << "ll_link" << endl
	     << ino << endl
	     << newparent << endl
	     << newname << endl;
    trace_lock.Unlock();
    
    //cout << "link " << path << " newpath " << newpath << endl;
    int res = ::link(path.c_str(), newpath.c_str());
    
    if (res == 0) {
	struct fuse_entry_param fe;
	memset(&fe, 0, sizeof(fe));
	::lstat(newpath.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
	fuse_reply_entry(req, &fe);
    } else 
	fuse_reply_err(req, errno);
}

static void ft_ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    string path;
    lock.Lock();
    make_ino_path(path, ino);
    lock.Unlock();
    
    int fd = ::open(path.c_str(), fi->flags);

    trace_lock.Lock();
    traceout << "ll_open" << endl
	     << ino << endl
	     << fi->flags << endl
	     << (fd > 0 ? fd:0) << endl;;
    trace_lock.Unlock();

    if (fd > 0) {
	fi->fh = fd;
	fuse_reply_open(req, fi);
    } else
	fuse_reply_err(req, errno);
}

static void ft_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		       struct fuse_file_info *fi)
{
    string path;
    lock.Lock();
    make_ino_path(path, ino);
    lock.Unlock();
    
    char *buf = new char[size];
    int res = ::pread(fi->fh, buf, size, off);

    trace_lock.Lock();
    traceout << "ll_read" << endl 
	     << fi->fh << endl
	     << off << endl
	     << size << endl;
    trace_lock.Unlock();

    if (res >= 0) 
	fuse_reply_buf(req, buf, res);
    else
	fuse_reply_err(req, errno);
    delete[] buf;
}

static void ft_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
			   size_t size, off_t off, struct fuse_file_info *fi)
{
    string path;
    lock.Lock();
    make_ino_path(path, ino);
    lock.Unlock();

    int res = ::pwrite(fi->fh, buf, size, off);

    trace_lock.Lock();
    traceout << "ll_write" << endl
	     << fi->fh << endl
	     << off << endl
	     << size << endl;
    trace_lock.Unlock();

    if (res >= 0) 
	fuse_reply_write(req, res);
    else
	fuse_reply_err(req, errno);
}

static void ft_ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    trace_lock.Lock();
    traceout << "ll_flush" << endl << fi->fh << endl;
    trace_lock.Unlock();

    int res = close(dup(fi->fh));
    if (res >= 0)
	fuse_reply_err(req, 0);
    else
	fuse_reply_err(req, errno);
}

static void ft_ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    trace_lock.Lock();
    traceout << "ll_release" << endl << fi->fh << endl;
    trace_lock.Unlock();

    int res = ::close(fi->fh);
    if (res >= 0)
	fuse_reply_err(req, 0);
    else
	fuse_reply_err(req, errno);
}

static void ft_ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
			  struct fuse_file_info *fi)
{
    trace_lock.Lock();
    traceout << "ll_fsync" << endl << fi->fh << endl;
    trace_lock.Unlock();

    int res = ::fsync(fi->fh);
    if (res >= 0)
	fuse_reply_err(req, 0);
    else
	fuse_reply_err(req, errno);
}

static struct fuse_lowlevel_ops ft_ll_oper = {
 init: 0,
 destroy: 0,
 lookup: ft_ll_lookup,
 forget: ft_ll_forget,
 getattr: ft_ll_getattr,
 setattr: ft_ll_setattr,
 readlink: ft_ll_readlink,
 mknod: ft_ll_mknod,
 mkdir: ft_ll_mkdir,
 unlink: ft_ll_unlink,
 rmdir: ft_ll_rmdir,
 symlink: ft_ll_symlink,
 rename: ft_ll_rename,
 link: ft_ll_link,
 open: ft_ll_open,
 read: ft_ll_read,
 write: ft_ll_write,
 flush: ft_ll_flush,
 release: ft_ll_release,
 fsync: ft_ll_fsync,
 opendir: ft_ll_opendir,
 readdir: ft_ll_readdir,
 releasedir: ft_ll_releasedir,
 fsyncdir: 0,
 statfs: ft_ll_statfs,
 setxattr: 0,
 getxattr: 0,
 listxattr: 0,
 removexattr: 0,
 access: 0,
 create: 0,//ft_ll_create,
 getlk: 0,
 setlk: 0,
 bmap: 0
};

int main(int argc, char *argv[])
{
    // open trace

    // figure base dir
    char *newargv[100];
    int newargc = 0;
    for (int i=0; i<argc; i++) {
	if (strcmp(argv[i], "--basedir") == 0) {
	    basedir = argv[++i];
	} else if (strcmp(argv[i], "--trace") == 0) {
	    tracefile.open(argv[++i], ios::out|ios::trunc);
	    if (!tracefile.is_open())
		cerr << "** couldn't open trace file " << argv[i] << endl;
	} else {
	    cout << "arg: " << newargc << " " << argv[i] << endl;
	    newargv[newargc++] = argv[i];
	}
    }
    newargv[newargc++] = "-o";
    newargv[newargc++] = "allow_other";
    if (!basedir) return 1;
    cout << "basedir is " << basedir << endl;

    // create root ino
    Inode *root = new Inode;
    lstat(basedir, &root->stbuf);
    root->stbuf.st_ino = 1;
    inode_map[1] = root;
    root->ref++;

    umask(0);
    
    // go go gadget fuse
    struct fuse_args args = FUSE_ARGS_INIT(newargc, newargv);
    struct fuse_chan *ch;
    char *mountpoint;
    int err = -1;
    
    if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1 &&
	(ch = fuse_mount(mountpoint, &args)) != NULL) {
	struct fuse_session *se;

	// init fuse
	se = fuse_lowlevel_new(&args, &ft_ll_oper, sizeof(ft_ll_oper),
			       NULL);
	if (se != NULL) {
	    if (fuse_set_signal_handlers(se) != -1) {
		fuse_session_add_chan(se, ch);
		err = fuse_session_loop(se);
		fuse_remove_signal_handlers(se);
		fuse_session_remove_chan(ch);
	    }
	    fuse_session_destroy(se);
	}
	fuse_unmount(mountpoint, ch);
    }
    fuse_opt_free_args(&args);
}
