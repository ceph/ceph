#ifndef _STATLITE_H
#define _STATLITE_H

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

#define S_REQUIRESIZE     1
#define S_REQUIREBLKSIZE  2
#define S_REQUIREBLOCKS   4
#define S_REQUIREATIME    8
#define S_REQUIREMTIME    16
#define S_REQUIRECTIME    32

#define S_ISVALIDSIZE(m)      (m & S_REQUIRESIZE)
#define S_ISVALIDBLKSIZE(m)   (m & S_REQUIREBLKSIZE)
#define S_ISVALIDBLOCKS(m)    (m & S_REQUIREBLOCKS)
#define S_ISVALIDATIME(m)     (m & S_REQUIREATIME)
#define S_ISVALIDMTIME(m)     (m & S_REQUIREMTIME)
#define S_ISVALIDCTIME(m)     (m & S_REQUIRECTIME)


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
