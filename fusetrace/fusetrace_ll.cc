// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:t -*- 
// vim: ts=8 sw=4 smarttab

/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.

    gcc -Wall `pkg-config fuse --cflags --libs` -lulockmgr fusexmp_fh.c -o fusexmp_fh
*/

#define FUSE_USE_VERSION 30

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
#include <time.h>


#include "include/unordered_map.h"
#include "include/hash_namespace.h"

#ifndef __LP64__
CEPH_HASH_NAMESPACE_START
  template<> struct hash<uint64_t> {
    size_t operator()(uint64_t __x) const { 
      static hash<uint32_t> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
CEPH_HASH_NAMESPACE_END
#endif


#include <iostream>
#include <fstream>
#include <map>
#include <set>
using namespace std;

#include "common/Mutex.h"

Mutex trace_lock;
ofstream tracefile;

#define traceout (tracefile.is_open() ? tracefile : cout)

char *basedir = 0;
int debug = 0;
bool do_timestamps = true;

#define dout if (debug) cout

Mutex lock;

struct Inode {
    struct stat stbuf;
    int ref;
    set<int> fds;

    map<pair<string,ino_t>,Inode*> parents;

    // if dir,
    map<string,Inode*> dentries;

    Inode() : ref(0) {}

    Inode *lookup(const string& dname) { 
	if (dentries.count(dname))
	    return dentries[dname];
	return 0;
    }
};

Inode *root = 0;
ceph::unordered_map<ino_t, Inode*> inode_map;

bool make_inode_path(string &buf, Inode *in)
{
    if (!in->parents.empty()) {
	if (!make_inode_path(buf, in->parents.begin()->second))
	    return false;
	buf += "/";
	buf += in->parents.begin()->first.first;
    } else {
	if (in != root) return false;
	assert(in->stbuf.st_ino == 1);
	buf = basedir;
	buf += "/";
    }
    return true;
    //dout << "path: " << in->stbuf.st_ino << " -> " << buf << endl;
}

bool make_inode_path(string &buf, Inode *in, const char *name)
{
    if (!make_inode_path(buf, in)) return false;
    buf += "/";
    buf += name;
    return true;
}

bool make_ino_path(string &buf, ino_t ino)
{
    Inode *in = inode_map[ino];
    assert(in);
    return make_inode_path(buf, in);
}

bool make_ino_path(string &buf, ino_t ino, const char *name)
{
    Inode *in = inode_map[ino];
    assert(in);
    if (!make_inode_path(buf, in))
	return false;
    buf += "/";
    buf += name;
    return true;
}

void remove_dentry(Inode *pin, const string& dname)
{
    dout << "remove_dentry " << pin->stbuf.st_ino << " " << dname << endl;

    Inode *in = pin->lookup(dname);
    assert(in);
    pin->dentries.erase(dname);
    in->parents.erase(pair<string,ino_t>(dname,pin->stbuf.st_ino));

    dout << "remove_dentry " << pin->stbuf.st_ino << " " << dname 
	 << " ... inode " << in->stbuf.st_ino << " ref " << in->ref
	 << endl;  
}

void add_dentry(Inode *parent, const string& dname, Inode *in)
{
    dout << "add_dentry " << parent->stbuf.st_ino << " " << dname << " to " << in->stbuf.st_ino << endl;

    if (parent->dentries.count(dname))
	remove_dentry(parent, dname);  // e.g., when renaming over another file..

    parent->dentries[dname] = in;
    in->parents[pair<string,ino_t>(dname,parent->stbuf.st_ino)] = parent;
}

void unlink_inode(Inode *in)
{
    dout << "unlink_inode " << in->stbuf.st_ino << " ref " << in->ref << endl;

    // remove parent links
    while (!in->parents.empty()) {
	Inode *parent = in->parents.begin()->second;
	string dname = in->parents.begin()->first.first;
	remove_dentry(parent, dname);
    }

    // remove children
    while (!in->dentries.empty()) 
	remove_dentry(in, in->dentries.begin()->first);

    while (!in->fds.empty()) {
	int fd = *in->fds.begin();
	::close(fd);
	in->fds.erase(in->fds.begin());
	dout << "remove_inode closeing stray fd " << fd << endl;
    }
}

void remove_inode(Inode *in)
{
    dout << "remove_inode " << in->stbuf.st_ino << " ref " << in->ref << endl;

    unlink_inode(in);

    inode_map.erase(in->stbuf.st_ino);
    dout << "remove_inode " << in->stbuf.st_ino << " done" << endl;
    delete in;
}

Inode *add_inode(Inode *parent, const char *name, struct stat *attr)
{
    dout << "add_inode " << parent->stbuf.st_ino << " " << name << " " << attr->st_ino << endl;

    Inode *in;
    if (inode_map.count(attr->st_ino)) {
	// reuse inode
	in = inode_map[attr->st_ino];
	unlink_inode(in);  // hrm.. should this close open fds?  probably.
	dout << "** REUSING INODE **" << endl;
    } else {
	inode_map[attr->st_ino] = in = new Inode;
    }
    memcpy(&in->stbuf, attr, sizeof(*attr));

    string dname(name);
    add_dentry(parent, dname, in);

    return in;
}


void print_time()
{
    if (do_timestamps) {
	struct timeval tv;
	gettimeofday(&tv, 0);
	traceout << "@" << endl
		 << tv.tv_sec << endl
		 << tv.tv_usec << endl;
    }
}


bool has_perm(int mask, Inode *in, int uid, int gid)
{
    dout << "hash_perm " << uid << "." << gid << " " << oct << mask << " in " << in->stbuf.st_mode
	 << " " << in->stbuf.st_uid << "." << in->stbuf.st_gid << endl;
    if (in->stbuf.st_mode & mask) return true;
    if (in->stbuf.st_gid == gid && in->stbuf.st_mode & (mask << 3)) return true;
    if (in->stbuf.st_uid == uid && in->stbuf.st_mode & (mask << 6)) return true;
    return false;
}


static void ft_ll_lookup(fuse_req_t req, fuse_ino_t pino, const char *name)
{
    int res = 0;

    //dout << "lookup " << pino << " " << name << endl;

    struct fuse_entry_param fe;
    memset(&fe, 0, sizeof(fe));

    lock.Lock();
    Inode *parent = inode_map[pino];
    assert(parent);

    // check permissions

    string dname(name);
    string path;
    Inode *in = 0;
    if (!has_perm(0001, parent, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) {
	res = EPERM;
    } 
    else if (!make_inode_path(path, parent, name)) {
	res = ENOENT;
    } else {
	in = parent->lookup(dname);
	if (in && res == 0) {
	    // re-stat, for good measure
	    res = ::lstat(path.c_str(), &in->stbuf);
	    
	    // hrm!
	    if (res != 0) {
		dout << "** WEIRD ** lookup on " << pino << " " << name << " inode went away!" << endl;
		in = 0;
		res = errno;
	    }
	    
	    //dout << "have " << in->stbuf.st_ino << endl;
	} else {
	    in = new Inode;
	    res = ::lstat(path.c_str(), &in->stbuf);
	    //dout << "stat " << path << " res = " << res << endl;
	    if (res == 0) {
		inode_map[in->stbuf.st_ino] = in;
		add_dentry(parent, dname, in);
	    } else {
		delete in;
		in = 0;
		res = errno;
	    }
	}
	if (in) {
	    in->ref++;
	    fe.ino = in->stbuf.st_ino;
	    memcpy(&fe.attr, &in->stbuf, sizeof(in->stbuf));
	}
    }
    lock.Unlock();

    trace_lock.Lock();
    print_time();
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
	    dout << "forget on " << ino << " ref " << in->ref << ", forget " << nlookup << endl;
	    if (in->ref < nlookup)
		dout << "**** BAD **** forget on " << ino << " ref " << in->ref << ", forget " << nlookup << endl;
	    
	    in->ref -= nlookup;
	    if (in->ref <= 0) 
		remove_inode(in);
	} else {
	    dout << "**** BAD **** forget " << nlookup << " on nonexistent inode " << ino << endl;
	}
	lock.Unlock();
    }

    trace_lock.Lock();
    print_time();
    traceout << "ll_forget" << endl << ino << endl << nlookup << endl;
    trace_lock.Unlock();

    fuse_reply_none(req);
}

static void ft_ll_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    int res = 0;
    string path;
    int fd = 0;

    Inode *in = 0;
    struct stat attr;

    lock.Lock();
    in = inode_map[ino];
    if (in->fds.empty()) {
	if (!make_inode_path(path, in))
	    res = ENOENT;
    } else
	fd = *in->fds.begin();
    lock.Unlock();

    if (fd > 0) {
	res = ::fstat(fd, &attr);
	dout << "getattr fstat on fd " << fd << " res " << res << endl;
    } else if (res == 0) {
	res = ::lstat(path.c_str(), &attr);
	dout << "getattr lstat on " << path << " res " << res << endl;
    }
    if (res < 0) res = errno;
    if (ino == 1) attr.st_ino = 1;
    
    trace_lock.Lock();
    print_time();
    traceout << "ll_getattr" << endl << ino << endl;
    trace_lock.Unlock();

    if (res == 0) {
	lock.Lock();
	memcpy(&in->stbuf, &attr, sizeof(attr));
	lock.Unlock();
	fuse_reply_attr(req, &attr, 0);
    } else 
	fuse_reply_err(req, res);
}

static void ft_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			  int to_set, struct fuse_file_info *fi)
{
    string path;
    Inode *in = 0;
    int fd = 0;
    int res = 0;

    lock.Lock();
    in = inode_map[ino];
    if (in->fds.empty() || (to_set & FUSE_SET_ATTR_MTIME)) {
	if (!make_inode_path(path, in))
	    res = ENOENT;
    } else
	fd = *in->fds.begin();
    lock.Unlock();

    trace_lock.Lock();
    print_time();
    traceout << "ll_setattr" << endl << ino << endl;
    traceout << attr->st_mode << endl;
    traceout << attr->st_uid << endl << attr->st_gid << endl;
    traceout << attr->st_size << endl;
    traceout << attr->st_mtime << endl;
    traceout << attr->st_atime << endl;
    traceout << to_set << endl;
    trace_lock.Unlock();

    if (res == 0 && !has_perm(0010, in, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) {
	res = EPERM;
    } else if (res == 0) {
	if (to_set & FUSE_SET_ATTR_MODE) {
	    if (fd > 0)
		res = ::fchmod(fd, attr->st_mode);
	    else
		res = ::chmod(path.c_str(), attr->st_mode);
	}
	if (!res && to_set & FUSE_SET_ATTR_UID) {
	    if (fd > 0) 
		res = ::fchown(fd, attr->st_uid, attr->st_gid);
	    else
		res = ::chown(path.c_str(), attr->st_uid, attr->st_gid);
	}
	if (!res && to_set & FUSE_SET_ATTR_SIZE) {
	    if (fd > 0) 
		res = ::ftruncate(fd, attr->st_size);
	    else
		res = ::truncate(path.c_str(), attr->st_size);
	}
	if (!res && to_set & FUSE_SET_ATTR_MTIME) {
	    struct utimbuf ut;
	    ut.actime = attr->st_atime;
	    ut.modtime = attr->st_mtime;
	    res = ::utime(path.c_str(), &ut);
	}
	if (res < 0) res = errno;
    }

    if (res == 0) {
	lock.Lock();
	::lstat(path.c_str(), &in->stbuf);
	if (ino == 1) in->stbuf.st_ino = 1;
	memcpy(attr, &in->stbuf, sizeof(*attr));
	lock.Unlock();
	fuse_reply_attr(req, attr, 0);
    } else
	fuse_reply_err(req, res);    
}


static void ft_ll_readlink(fuse_req_t req, fuse_ino_t ino)
{
    string path;
    int res = 0;

    lock.Lock();
    if (!make_ino_path(path, ino))
	res = ENOENT;
    lock.Unlock();

    trace_lock.Lock();
    print_time();
    traceout << "ll_readlink" << endl << ino << endl;
    trace_lock.Unlock();

    char buf[256];
    if (res == 0) res = readlink(path.c_str(), buf, 255);
    if (res < 0) res = errno;

    if (res >= 0) {
	buf[res] = 0;
	fuse_reply_readlink(req, buf);
    } else {
	fuse_reply_err(req, res);
    }
}


static void ft_ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    string path;
    int res = 0;
    lock.Lock();
    Inode *in = inode_map[ino];
    if (!make_inode_path(path, in))
	res = ENOENT;
    lock.Unlock();

    DIR *dir = 0;
    if (res == 0 && !has_perm(0100, in, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) 
	res = EPERM;
    else if (res == 0) dir = opendir(path.c_str());
    if (res < 0) res = errno;

    trace_lock.Lock();
    print_time();
    traceout << "ll_opendir" << endl << ino << endl << (unsigned long)dir << endl;
    trace_lock.Unlock();
    
    if (dir) {
	fi->fh = (long)dir;
	fuse_reply_open(req, fi);
    } else 
	fuse_reply_err(req, res);
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
    print_time();
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
    int res = 0;
    lock.Lock();
    pin = inode_map[parent];
    if (!make_inode_path(path, pin, name))
	res = ENOENT;
    lock.Unlock();

    dout << "mknod " << path << endl;
    if (res == 0 && !has_perm(0010, pin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) 
	res = EPERM;
    else if (res == 0) res = ::mknod(path.c_str(), mode, rdev);
    if (res < 0) 
	res = errno;
    else
	::chown(path.c_str(), fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid);

    struct fuse_entry_param fe;
    if (res == 0) {
	memset(&fe, 0, sizeof(fe));
	::lstat(path.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
	lock.Lock();
	Inode *in = add_inode(pin, name, &fe.attr);
	in->ref++;
	lock.Unlock();
    }

    trace_lock.Lock();
    print_time();
    traceout << "ll_mknod" << endl << parent << endl << name << endl << mode << endl << rdev << endl;
    traceout << (res == 0 ? fe.ino:0) << endl;
    trace_lock.Unlock();

    if (res == 0)
	fuse_reply_entry(req, &fe);
    else 
	fuse_reply_err(req, res);
}

static void ft_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
			mode_t mode)
{
    string path;
    Inode *pin = 0;
    int res = 0;
    lock.Lock();
    pin = inode_map[parent];
    if (!make_inode_path(path, pin, name))
	res = ENOENT;
    lock.Unlock();

    if (res == 0 && !has_perm(0010, pin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) 
	res = EPERM;
    else if (res == 0) res = ::mkdir(path.c_str(), mode);
    if (res < 0)
	res = errno;
    else
	::chown(path.c_str(), fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid);

    struct fuse_entry_param fe;
    if (res == 0) {
	memset(&fe, 0, sizeof(fe));
	::lstat(path.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
  	lock.Lock();
	Inode *in = add_inode(pin, name, &fe.attr);
	in->ref++;
  	lock.Unlock();
    }

    trace_lock.Lock();
    print_time();
    traceout << "ll_mkdir" << endl << parent << endl << name << endl << mode << endl;
    traceout << (res == 0 ? fe.ino:0) << endl;
    trace_lock.Unlock();

    if (res == 0)
	fuse_reply_entry(req, &fe);
    else 
	fuse_reply_err(req, res);
}

static void ft_ll_symlink(fuse_req_t req, const char *value, fuse_ino_t parent, const char *name)
{
    string path;
    Inode *pin = 0;
    int res = 0;

    lock.Lock();
    pin = inode_map[parent];
    if (!make_inode_path(path, pin, name))
	res = ENOENT;
    lock.Unlock();

    if (res == 0 && !has_perm(0010, pin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) 
	res = EPERM;
    else if (res == 0) res = ::symlink(value, path.c_str());
    if (res < 0) 
	res = errno;
    else
	::chown(path.c_str(), fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid);

    struct fuse_entry_param fe;
    if (res == 0) {
	memset(&fe, 0, sizeof(fe));
	::lstat(path.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
      	lock.Lock();
	Inode *in = add_inode(pin, name, &fe.attr);
	in->ref++;
    	lock.Unlock();
    }

    trace_lock.Lock();
    print_time();
    traceout << "ll_symlink" << endl << parent << endl << name << endl << value << endl;
    traceout << (res == 0 ? fe.ino:0) << endl;
    trace_lock.Unlock();

    if (res == 0)
	fuse_reply_entry(req, &fe);
    else 
	fuse_reply_err(req, res);
}

static void ft_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name,
			 mode_t mode, struct fuse_file_info *fi)
{
    string path;
    Inode *pin = 0;
    int res = 0;

    lock.Lock();
    pin = inode_map[parent];
    if (!make_inode_path(path, pin, name))
	res = ENOENT;
    lock.Unlock();

    dout << "create " << path << endl;
    int fd = 0;
    if (res == 0 && !has_perm(0010, pin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) 
	res = EPERM;
    else if (res == 0) {
	fd = ::open(path.c_str(), fi->flags|O_CREAT, mode);
	if (fd < 0) {
	    res = errno;
	} else {
	    ::fchown(fd, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid);
	}
    }

    struct fuse_entry_param fe;
    memset(&fe, 0, sizeof(fe));
    if (res == 0) {
	::lstat(path.c_str(), &fe.attr);
	fe.ino = fe.attr.st_ino;
	lock.Lock();
	Inode *in = add_inode(pin, name, &fe.attr);
	in->ref++;
	in->fds.insert(fd);
	lock.Unlock();
	fi->fh = fd;
    }

    trace_lock.Lock();
    print_time();
    traceout << "ll_create" << endl
	     << parent << endl
	     << name << endl 
	     << mode << endl
	     << fi->flags << endl
	     << (res == 0 ? fd:0) << endl
	     << fe.ino << endl;
    trace_lock.Unlock();

    if (res == 0)
	fuse_reply_create(req, &fe, fi);
    else 
	fuse_reply_err(req, res);

}


static void ft_ll_statfs(fuse_req_t req, fuse_ino_t ino)
{
    string path;
    int res = 0;
    if (ino) {
	lock.Lock();
	if (!make_ino_path(path, ino))
	    res = ENOENT;
	lock.Unlock();
    } else {
	path = basedir;
    }

    trace_lock.Lock();
    print_time();
    traceout << "ll_statfs" << endl << ino << endl;
    trace_lock.Unlock();
    
    struct statvfs stbuf;
    if (res == 0) res = statvfs(path.c_str(), &stbuf);
    if (res < 0) res = errno;

    if (res == 0) 
	fuse_reply_statfs(req, &stbuf);
    else 
	fuse_reply_err(req, res);
}

static void ft_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    string path;
    Inode *pin = 0;
    Inode *in = 0;
    string dname(name);
    int res = 0;
    lock.Lock();
    pin = inode_map[parent];
    in = pin->lookup(dname);
    if (!make_inode_path(path, pin, name))
	res = ENOENT;
    lock.Unlock();

    trace_lock.Lock();
    print_time();
    traceout << "ll_unlink" << endl << parent << endl << name << endl;
    trace_lock.Unlock();
    
    if (res == 0 && !has_perm(0010, pin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) 
	res = EPERM;
    else if (res == 0) {
	if (in && in->fds.empty()) {
	    int fd = ::open(path.c_str(), O_RDWR);
	    if (fd > 0)
		in->fds.insert(fd);  // for slow getattrs.. wtf
	    dout << "unlink opening paranoia fd " << fd << endl;
	}
	res = ::unlink(path.c_str());
	if (res < 0) res = errno;
    }

    if (res == 0) {
	// remove from out cache
  	lock.Lock();
	string dname(name);
	if (pin->lookup(dname))
	    remove_dentry(pin, dname);
	lock.Unlock();
	fuse_reply_err(req, 0);
    } else
	fuse_reply_err(req, res);
}

static void ft_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    string path;
    Inode *pin = 0;
    int res = 0;

    lock.Lock();
    pin = inode_map[parent];
    if (!make_inode_path(path, pin, name))
	res = ENOENT;
    lock.Unlock();

    trace_lock.Lock();
    print_time();
    traceout << "ll_rmdir" << endl << parent << endl << name << endl;
    trace_lock.Unlock();

    if (res == 0 && !has_perm(0010, pin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)) 
	res = EPERM;
    else if (res == 0) res = ::rmdir(path.c_str());
    if (res < 0) res = errno;

    if (res == 0) {
	// remove from out cache
	lock.Lock();
	string dname(name);
	if (pin->lookup(dname))
	    remove_dentry(pin, dname);
	lock.Unlock();
	fuse_reply_err(req, 0);
    } else
	fuse_reply_err(req, res);
}


static void ft_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
			 fuse_ino_t newparent, const char *newname)
{
    string path;
    string newpath;
    Inode *pin = 0;
    Inode *newpin = 0;
    int res = 0;
    lock.Lock();
    pin = inode_map[parent];
    if (!make_inode_path(path, pin, name))
	res = ENOENT;
    newpin = inode_map[newparent];
    if (!make_inode_path(newpath, newpin, newname))
	res = ENOENT;
    lock.Unlock();

    trace_lock.Lock();
    print_time();
    traceout << "ll_rename" << endl
	     << parent << endl 
	     << name << endl
	     << newparent << endl
	     << newname << endl;
    trace_lock.Unlock();

    if (res == 0 && (!has_perm(0010, pin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid) ||
		     !has_perm(0010, newpin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)))
	res = EPERM;
    else if (res == 0) res = ::rename(path.c_str(), newpath.c_str());
    if (res < 0) res = errno;
    
    if (res == 0) {
	string dname(name);
	string newdname(newname);
	lock.Lock();
	Inode *in = pin->lookup(dname);
	if (in) {
	    add_dentry(newpin, newdname, in);
	    remove_dentry(pin, dname);
	} else {
	    dout << "hrm, rename didn't have renamed inode.. " << path << " to " << newpath << endl;
	}
	lock.Unlock();
	fuse_reply_err(req, 0);
    } else 
	fuse_reply_err(req, res);
}

static void ft_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
		       const char *newname)
{
    string path;
    string newpath;
    Inode *in = 0;
    Inode *newpin = 0;
    int res = 0;
    lock.Lock();
    in = inode_map[ino];
    if (!make_inode_path(path, in))
	res = ENOENT;

    newpin = inode_map[newparent];
    if (!make_inode_path(newpath, newpin, newname))
	res = ENOENT;
    lock.Unlock();

    trace_lock.Lock();
    print_time();
    traceout << "ll_link" << endl
	     << ino << endl
	     << newparent << endl
	     << newname << endl;
    trace_lock.Unlock();
    
    //cout << "link " << path << " newpath " << newpath << endl;
    if (res == 0 && (!has_perm(0010, in, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid) ||
		     !has_perm(0010, newpin, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid)))
	res = EPERM;
    else if (res == 0) res = ::link(path.c_str(), newpath.c_str());
    if (res < 0) res = errno;
    
    if (res == 0) {
	struct fuse_entry_param fe;
	memset(&fe, 0, sizeof(fe));
	::lstat(newpath.c_str(), &fe.attr);

	lock.Lock();
	string newdname(newname);
	add_dentry(newpin, newdname, in);
	in->ref++;
	memcpy(&in->stbuf, &fe.attr, sizeof(fe.attr));   // re-read, bc we changed the link count
	lock.Unlock();

	fe.ino = fe.attr.st_ino;
	fuse_reply_entry(req, &fe);
    } else 
	fuse_reply_err(req, res);
}

static void ft_ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    string path;
    Inode *in = 0;
    int res = 0;

    lock.Lock();
    in = inode_map[ino];
    if (!make_inode_path(path, in))
	res = ENOENT;
    lock.Unlock();
    
    int want = 0100;
    if (fi->flags & O_RDWR) want |= 0010;
    if (fi->flags == O_WRONLY) want = 0010;

    int fd = 0;
    if (res == 0 && !has_perm(want, in, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid))
	res = EPERM;
    else if (res == 0) {
	fd = ::open(path.c_str(), fi->flags);
	if (fd <= 0) res = errno;
    }

    trace_lock.Lock();
    print_time();
    traceout << "ll_open" << endl
	     << ino << endl
	     << fi->flags << endl
	     << (fd > 0 ? fd:0) << endl;;
    trace_lock.Unlock();

    if (res == 0) {
	lock.Lock();
	in->fds.insert(fd);
	lock.Unlock();
	fi->fh = fd;
	fuse_reply_open(req, fi);
    } else
	fuse_reply_err(req, res);
}

static void ft_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		       struct fuse_file_info *fi)
{
   
    char *buf = new char[size];
    int res = ::pread(fi->fh, buf, size, off);

    //cout << "read " << path << " " << off << "~" << size << endl;
    trace_lock.Lock();
    print_time();
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
    int res = ::pwrite(fi->fh, buf, size, off);

    trace_lock.Lock();
    print_time();
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
    print_time();
    traceout << "ll_flush" << endl << fi->fh << endl;
    trace_lock.Unlock();

    int res = ::fdatasync(fi->fh);
    //int res = ::close(dup(fi->fh));
    if (res >= 0)
	fuse_reply_err(req, 0);
    else
	fuse_reply_err(req, errno);
}

static void ft_ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    trace_lock.Lock();
    print_time();
    traceout << "ll_release" << endl << fi->fh << endl;
    trace_lock.Unlock();

    lock.Lock();
    Inode *in = inode_map[ino];
    in->fds.erase(fi->fh);
    lock.Unlock();

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
    print_time();
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
 create: ft_ll_create,
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
	} else if (strcmp(argv[i], "--timestamps") == 0) {
	    do_timestamps = atoi(argv[++i]);
	} else if (strcmp(argv[i], "--trace") == 0) {
	    tracefile.open(argv[++i], ios::out|ios::trunc);
	    if (!tracefile.is_open())
		cerr << "** couldn't open trace file " << argv[i] << endl;
	} else if (strcmp(argv[i], "--debug") == 0) {
	    debug = 1;
	} else {
	    cout << "arg: " << newargc << " " << argv[i] << endl;
	    newargv[newargc++] = argv[i];
	}
    }
    newargv[newargc++] = "-o";
    newargv[newargc++] = "allow_other";
    //    newargv[newargc++] = "-o";
    //    newargv[newargc++] = "default_permissions";
    if (!basedir) return 1;
    cout << "basedir is " << basedir << endl;

    // create root ino
    root = new Inode;
    ::lstat(basedir, &root->stbuf);
    root->stbuf.st_ino = 1;
    inode_map[1] = root;
    root->ref++;

    umask(0);
    
    // go go gadget fuse
    struct fuse_args args = FUSE_ARGS_INIT(newargc, newargv);
    struct fuse_chan *ch;
    char *mountpoint;
    
    if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1 &&
	(ch = fuse_mount(mountpoint, &args)) != NULL) {
	struct fuse_session *se;

	// init fuse
	se = fuse_lowlevel_new(&args, &ft_ll_oper, sizeof(ft_ll_oper),
			       NULL);
	if (se != NULL) {
	    if (fuse_set_signal_handlers(se) != -1) {
		fuse_session_add_chan(se, ch);
		if (fuse_session_loop(se) <= -1) {
		    cout << "Failed fuse_session_loop() call." << endl;
		    return 1;
		}
		fuse_remove_signal_handlers(se);
		fuse_session_remove_chan(ch);
	    }
	    fuse_session_destroy(se);
	}
	fuse_unmount(mountpoint, ch);
    }
    fuse_opt_free_args(&args);
}
