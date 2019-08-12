// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_STATLITE_H
#define CEPH_STATLITE_H

extern "C" {

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

struct statlite {
  dev_t         st_dev;      /* device */
  ino_t         st_ino;      /* inode */
  mode_t        st_mode;     /* protection */
  nlink_t       st_nlink;    /* number of hard links */
  uid_t         st_uid;      /* user ID of owner */
  gid_t         st_gid;      /* group ID of owner */
  dev_t         st_rdev;     /* device type (if inode device)*/
  unsigned long st_litemask; /* bit mask for optional fields */
  /***************************************************************/
  /**** Remaining fields are optional according to st_litemask ***/
  off_t         st_size;     /* total size, in bytes         */
  blksize_t     st_blksize;  /* blocksize for filesystem I/O */
  blkcnt_t      st_blocks;   /* number of blocks allocated   */
  struct timespec st_atim;            /* Time of last access.  */
  struct timespec st_mtim;            /* Time of last modification.  */
  struct timespec st_ctim;            /* Time of last status change.  */
  //time_t        st_atime;    /* time of last access          */
  //time_t        st_mtime;    /* time of last modification    */
  //time_t        st_ctime;    /* time of last change          */
}; 

#define S_STATLITE_SIZE     1
#define S_STATLITE_BLKSIZE  2
#define S_STATLITE_BLOCKS   4
#define S_STATLITE_ATIME    8
#define S_STATLITE_MTIME    16
#define S_STATLITE_CTIME    32

#define S_REQUIRESIZE(m)      (m | S_STATLITE_SIZE)
#define S_REQUIREBLKSIZE(m)   (m | S_STATLITE_BLKSIZE)
#define S_REQUIREBLOCKS(m)    (m | S_STATLITE_BLOCKS)
#define S_REQUIREATIME(m)     (m | S_STATLITE_ATIME)
#define S_REQUIREMTIME(m)     (m | S_STATLITE_MTIME)
#define S_REQUIRECTIME(m)     (m | S_STATLITE_CTIME)

#define S_ISVALIDSIZE(m)      (m & S_STATLITE_SIZE)
#define S_ISVALIDBLKSIZE(m)   (m & S_STATLITE_BLKSIZE)
#define S_ISVALIDBLOCKS(m)    (m & S_STATLITE_BLOCKS)
#define S_ISVALIDATIME(m)     (m & S_STATLITE_ATIME)
#define S_ISVALIDMTIME(m)     (m & S_STATLITE_MTIME)
#define S_ISVALIDCTIME(m)     (m & S_STATLITE_CTIME)


// readdirplus etc.

struct dirent_plus {
 struct dirent     d_dirent;  /* dirent struct for this entry */
 struct stat       d_stat;    /* attributes for this entry */
 int               d_stat_err;/* errno for d_stat, or 0 */
};
struct dirent_lite {
 struct dirent     d_dirent;  /* dirent struct for this entry */
 struct statlite   d_stat;    /* attributes for this entry */
 int               d_stat_err;/* errno for d_stat, or 0 */
};

}
#endif
