/* Copyright (C) 2001 International Business Machines
 * All rights reserved.                                                         
 *                                                                              
 * This file is part of the GPFS user library.                                  
 *                                                                              
 * Redistribution and use in source and binary forms, with or without           
 * modification, are permitted provided that the following conditions           
 * are met:                                                                     
 *                                                                              
 *  1. Redistributions of source code must retain the above copyright notice,   
 *     this list of conditions and the following disclaimer.                    
 *  2. Redistributions in binary form must reproduce the above copyright        
 *     notice, this list of conditions and the following disclaimer in the      
 *     documentation and/or other materials provided with the distribution.     
 *  3. The name of the author may not be used to endorse or promote products    
 *     derived from this software without specific prior written                
 *     permission.                                                              
 *                                                                              
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR         
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES    
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.      
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;  
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,     
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR      
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF       
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.                                   
 *                                                                              
 */
/* %Z%%M%       %I%  %W% %G% %U% */

/*
 * GPFS interface definitions for supporting I/O hints and directives.
 *
 * Usage: The argument to gpfs_fcntl is composed of the concatenation of
 * structures defined in this file.  The first structure must be of type
 * gpfsFcntlHeader_t.  This is immediately followed by additional
 * structures, one for each hint or directive supplied.  The totalLength
 * field of the header contains the length of all of the structures,
 * including the header itself.  Each structure is defined to be a multiple
 * of 8 bytes in length, and the highest alignment requirement of any of the
 * data types is also 8 bytes, so the compiler will not insert padding when
 * several structures are declared within an outer structure.  This makes
 * it easier to build up the necessary area before calling gpfs_fcntl.
 *
 * For example, the following code fragment first releases all cached data
 * held on behalf of a file, then tells GPFS that this node will write
 * the portion of the file with file offsets between 2G and 3G-1:
 *   struct
 *   {
 *     gpfsFcntlHeader_t hdr;
 *     gpfsClearFileCache_t rel;
 *     gpfsAccessRange_t acc;
 *   } arg;
 *
 *   arg.hdr.totalLength = sizeof(arg);
 *   arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
 *   arg.hdr.fcntlReserved = 0;
 *   arg.rel.structLen = sizeof(arg.rel);
 *   arg.rel.structType = GPFS_CLEAR_FILE_CACHE;
 *   arg.acc.structLen = sizeof(arg.acc);
 *   arg.acc.structType = GPFS_ACCESS_RANGE;
 *   arg.acc.start = 2LL * 1024LL * 1024LL * 1024LL;
 *   arg.acc.length = 1024 * 1024 * 1024;
 *   arg.acc.isWrite = 1;
 *   rc = gpfs_fcntl(handle, &arg);
 *
 * If gpfs_fcntl returns an error (rc -1), errno will contain the error
 * reason, and the errorOffset field of the header will contain the offset
 * of the offending structure within the argument area.
 *
 * In general, the structures within the argument are processed in order.
 */

#ifndef _h_gpfs_fcntl
#define _h_gpfs_fcntl

/* open source interfaces */
#include "gpfs.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* Header of the parameter area passed to gpfs_fcntl */
typedef struct
{
  int           totalLength;    /* length of this structure plus the sum of
                                   the lengths of all structures in this
                                   gpfs_fcntl argument */
  int           fcntlVersion;   /* version number: GPFS_FCNTL_CURRENT_VERSION */
  int           errorOffset;    /* returned value giving offset into parameter
                                   area of the structure to which errno
                                   pertains.  Only set if errno is set. */
  int           fcntlReserved;  /* not used, should be set to 0 */
} gpfsFcntlHeader_t;

/* Moved it here from tshints.C, since function tschattr also uses it while 
 * filling in errReason into argument structure. */
typedef struct
{
    int structLen;    /* length of the entire argument */
    int structType;   /* identifier of the hint */
} genericStruct_t;


/* Interface version number (fcntlVersion field of gpfsFcntlHeader_t) */
#define GPFS_FCNTL_CURRENT_VERSION      1

/* Maximum length of argument to gpfs_fcntl */
#define GPFS_MAX_FCNTL_LENGTH       65536

/* Maximum length of a name argument passed to or returned from gpfs_fcntl.
   Length of buffer must be a multiple of 8. */
#define GPFS_FCNTL_MAX_NAME_BUFFER  1024 
#define GPFS_FCNTL_MIN_NAME_BUFFER  8


/* Definitions of structType fields for GPFS hints.  Hints can be ignored
   by GPFS without affecting correct operation, although performance might
   suffer. */
#define GPFS_ACCESS_RANGE            1001
#define GPFS_FREE_RANGE              1002
#define GPFS_MULTIPLE_ACCESS_RANGE   1003
#define GPFS_CLEAR_FILE_CACHE        1004
#define GPFS_FINE_GRAIN_WRITE_SHARING   1005
#define GPFS_PREFETCH                1006
#define GPFS_FINE_GRAIN_READ_SHARING    1007
#define GPFS_CREATE_SHARING          1008
#define GPFS_PURGE_FILE_AND_STAT_CACHE  1009
#define GPFS_DISABLE_INODE_PREFETCH  1010

/* Definitions of structType fields for GPFS directives.  GPFS must honor
   directives, or return an error saying why a directive could not be
   honored. */
#define GPFS_CANCEL_HINTS            2001
#define GPFS_FCNTL_SET_REPLICATION   2005
#define GPFS_FCNTL_SET_STORAGEPOOL   2006
#define GPFS_FCNTL_RESTRIPE_DATA     2007
#define GPFS_FCNTL_RESTRIPE_RANGE    2008

#define GPFS_FCNTL_FSYNC_RANGE       2009
#define GPFS_FCNTL_COMPRESSION_ON    2010
#define GPFS_FCNTL_COMPRESSION_OFF   2011
#define GPFS_FCNTL_COMPRESSION_LIB   2012

/* Definitions of structType fields for GPFS inquiries. Inquiries merely
   return GPFS attributes of existing files. */
#define GPFS_FCNTL_GET_REPLICATION       3001
#define GPFS_FCNTL_GET_STORAGEPOOL       3002
#define GPFS_FCNTL_GET_FILESETNAME       3003
#define GPFS_FCNTL_GET_SNAPSHOTNAME      3004
#define GPFS_FCNTL_GET_DATABLKDISKIDX    3005  /* obsoleted */
#define GPFS_FCNTL_GET_DATABLKLOC        3006
#define GPFS_FCNTL_GET_COMPRESSION       3007
#define GPFS_FCNTL_GET_COMPRESSION_SUPPORT 3008
#define GPFS_FCNTL_GET_SNAP_MIGRATION_SUPPORT 3009
/* for use with mmlsattr -D */
#define GPFS_FCNTL_GET_DATA_BLOCK_DISK_NUMS   3010
#define GPFS_FCNTL_SET_REPLICATIONX           3011

/* Structures for specifying the various gpfs_fcntl hints */

/* Access range hint:  The application will soon access file offsets within
   the given range, and will not access offsets outside the range.  Violating
   this hint may produce worse performance than if no hint was specified. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_ACCESS_RANGE */
  long long     start;          /* start offset in bytes from beginning of file */
  long long     length;         /* length of range; 0 indicates to end of file */
  int           isWrite;        /* 0 - read access, 1 - write access */
  char          padding[4];
} gpfsAccessRange_t;


/* Free range hint: the application will no longer access file offsets
   within the given range, so GPFS is free to flush those file offsets from
   its cache. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_FREE_RANGE */
  long long     start;          /* start offset in bytes from beginning of file */
  long long     length;         /* length of range; 0 indicates to end of file */
} gpfsFreeRange_t;


/* Format of accRangeArray and relRangeArray entries used by
   GPFS_MULTIPLE_ACCESS_RANGE hint */
typedef struct
{
  long long     blockNumber;    /* data block number to access */
  int           start;          /* start of range (from beginning of block) */
  int           length;         /* number of bytes in the range  */
  int           isWrite;        /* 0 - read access, 1 - write access */
  char          padding[4];
} gpfsRangeArray_t;

/* Fine Grain Write Sharing hint: This hint is used to optimize the performance
   of small strided writes to a shared file from a parallel application, for
   example as seen in the io500 benchmark, ior hard write. The hint should be
   issued on every file descriptor that will be used to write to the file.
   Using this hint, write() calls will be processed asynchronously.  That means
   after successful return from the write() call, read() and stat() may return
   old data and information for a while, and some error notifications may be
   delayed, possibly up until the time of file close.  All pending writes will
   be completed when the application issues hint to stop Fine Grain Write Sharing,
   calls fsync(), or when the file descriptor on which the hint was issued is closed.
   If disabling the Fine Grain Write Sharing hint, fsync(), or close() calls returns
   an error, some data may have been lost even if all previous write() calls
   returned successfully. Otherwise, all subsequent read() and stat() calls are
   guaranteed to return up-to-date data and information. Note that this is just a
   hint to GPFS and certain operations, such as O_SYNC, AIO, Direct IO, and mmap,
   are not compatible with this mode of operation. If one of these other options is
   used the fine-grain write sharing hint is ignored. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier:
                                   GPFS_FINE_GRAIN_WRITE_SHARING */
  int           fineGrainWriteSharing;      /* 0 - disable, 1 - enable */
  int           taskId;         /* optional: parallel job task id;
                                   set to -1 to ignore */
  int           totalTasks;     /* optional: total number of tasks in a
                                   parallel job; set to -1 to ignore */
  int           recordSize;     /* optional: size of each record;
                                   set to -1 to ignore */
} gpfsFineGrainWriteSharing_t;

/* Fine Grain Read Sharing hint:
   This hint is used to optimize the performance of multiple nodes/tasks
   issuing small (less than full block) strided reads to a shared file from
   a parallel application, for example as seen in the io500 benchmark,
   ior hard read. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier:
                                   GPFS_FINE_GRAIN_READ_SHARING */
  int           fineGrainReadSharing;      /* 0 - disable, 1 - enable */
  char          padding[4];
} gpfsFineGrainReadSharing_t;

/* Prefetch hint: This hint is used to enable and disable the normal full block
   prefetch and writebehind for sequential, fuzzy-sequential, and strided file
   access. The hint is to be issued on a file descriptor that will be used to
   read from or write to a file. */
typedef struct
{
  int           structLen;               /* length of this structure */
  int           structType;              /* hint identifier: GPFS_PREFETCH */
  int           prefetchEnableRead ;     /* 0 - disable, 1 - enable */
  int           prefetchEnableWrite ;    /* 0 - disable, 1 - enable */
} gpfsPrefetch_t;

/* Create Sharing hint: This hint optimizes the performance of multiple
   nodes/tasks creating many files in a shared directory from a parallel
   application, for example as seen in the io500 benchmark, mdtest hard write. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_CREATE_SHARING */
  int           enable;         /* 0 - disable, 1 - enable */
  char          padding[4];
} gpfsCreateSharing_t;

/* Purge File and Stat Cache hint:
   This hint attempts to remove all cached files & stat cache entries and
   relinquish the associated tokens. Files that are currently open or actively
   being accessed at the time the hint is issued are left untouched.

   Using this hint will benefit the applications that, after calling the hint,
   will access files that have not been accessed previously, therefore not
   taking advantage of file and stat caching. Additionally, if currently
   cached files are expected to be accessed on other nodes only, then issuing
   this hint may be beneficial by reducing token conflicts. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_PURGE_FILE_AND_STAT_CACHE */
} gpfsPurgeFileAndStatCache_t;

/* Disable Inode Prefetch hint:
   This hint disables prefetch of inodes cluster-wide or for a specific
   directory.

   Inode prefetch is optimized for "ls -l" operations, but may be
   counterproductive for applications that only access a small fraction
   of files in a large directory or access files in an order different from
   the order returned by "readdir". Workloads with such access patterns may
   benefit from this hint. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_DISABLE_INODE_PREFETCH */
  int           enable;         /* 0 - disable, 1 - enable for a directory,
                                   11 - enable globally */
  char          padding[4];
} gpfsDisableInodePrefetch_t;

/* Multiple access range hint: This hint is used to drive application-defined
   prefetching and writebehind.  The application will soon access the
   portions of the blocks specified in accRangeArray, and has finished
   accessing the ranges listed in relRangeArray.  The size of a block is
   returned by stat in the st_blksize field, so offset OFF of a file is in
   block OFF/st_blksize.  Up to GPFS_MAX_RANGE_COUNT blocks may be given in
   one multiple access range hint.  Depending on the current load, GPFS may
   initiate prefetching of some or all of these.  Each range named in
   accRangeArray that is accepted for prefetching should eventually be
   released via relRangeArray, or else GPFS will stop prefetching blocks
   for this file. */
#define GPFS_MAX_RANGE_COUNT 8
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_MULTIPLE_ACCESS_RANGE */
  int           accRangeCnt;    /* on input, number of ranges in accRangeArray
                                   on output, number of processed ranges (the
                                   first n of the given ranges) */
  int           relRangeCnt;    /* number of ranges in relRangeArray */
  gpfsRangeArray_t accRangeArray[GPFS_MAX_RANGE_COUNT]; /* requested ranges */
  gpfsRangeArray_t relRangeArray[GPFS_MAX_RANGE_COUNT]; /* ranges to release */
} gpfsMultipleAccessRange_t;


/* Clear file cache hint: the application expects to no longer access any
   portion of the file, so GPFS should flush and invalidate any cached
   data belonging to this file.  This may avoid synchronous cache invalidations
   on later uses of the file by other nodes. */
typedef struct
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_CLEAR_FILE_CACHE */
} gpfsClearFileCache_t;

/* Structures for specifying the various gpfs_fcntl directives */

/* Cancel all hints: GPFS removes any hints that may have been issued
   against this file.  Does not affect the contents of the GPFS file cache.
   Note that this directive does not cancel the effects of other directives,
   such as GPFS_CANCEL_HINTS. */
typedef struct  /* cancelAccessHints hint */
{
  int           structLen;      /* length of this structure */
  int           structType;     /* hint identifier: GPFS_CANCEL_HINTS */
} gpfsCancelHints_t;


/* This directive is used to set a file's replication factors.
   However, the directive does not cause the file data to be restriped 
   immediately. Instead the caller must append a gpfsRestripeData_t directive
   or invoke an mmrestripefs or an mmrestripefile command. */
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: 
                                  GPFS_FCNTL_SET_REPLICATIONX or
                                  GPFS_FCNTL_SET_REPLICATION */
  int metadataReplicas;        /* Set the number of copies of the file's 
                                  indirect blocks. Valid values are 1-3,
                                  but not greater than the value of 
                                  maxMetadataReplicas. A value of 0 indicates
                                  not to change the current value. */  
  int maxMetadataReplicas;     /* Set the maximum number of copies of a file's
                                  indirect blocks. Space in the file's inode
                                  and indirect blocks is reserved for the
                                  maximum number of copies, regardless of the
                                  current value. Valid values are 1-3.
                                  A value of 0 indicates not to change the 
                                  current value. */
  int dataReplicas;            /* Set the number of copies of the file's
                                  data blocks. Valid values are 0-3,
                                  but cannot be greater than the value of
                                  maxDataReplicas. A value of 0 indicates
                                  not to change the current value under
                                  GPFS_FCNTL_SET_REPLICATION.
                                  Under GPFS_FCNTL_SET_REPLICATIONX, a valiue
                                  of -1 indicates no change to the current
                                  values.  */
  int maxDataReplicas;         /* Set the maximum number of copies of a file's
                                  data blocks. Space in the file's inode
                                  and indirect blocks is reserved for the
                                  maximum number of copies, regardless of the
                                  current value. Valid values are 1-3. 
                                  A value of 0 indicates not the change the
                                  current value. */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int perfReplicas;            /* Under GPFS_FCNTL_SET_REPLICATIONX, set the
                                  number of additonal copies of the file's data
                                  blocks for performance. Valid values are 0-1.
                                  A value of -1 indicates no change to the
                                  current value.  */
} gpfsSetReplication_t;



/* Values that may be returned by errReason */

/* No reason information was returned. */
#define GPFS_FCNTL_ERR_NONE                       0

/* MetadataReplicas is out of range.
   errValue1 and errValue2 contain the valid lower and upper range boundaries. */
#define GPFS_FCNTL_ERR_METADATA_REPLICAS_RANGE    1

/* MaxMetadataReplicas is out of range.
   errValue1 and errValue2 contain the valid lower and upper range boundaries. */
#define GPFS_FCNTL_ERR_MAXMETADATA_REPLICAS_RANGE 2

/* DataReplicas is out of range.
   errValue1 and errValue2 contain the valid lower and upper range boundaries. */
#define GPFS_FCNTL_ERR_DATA_REPLICAS_RANGE        3

/* MaxDataReplicas is out of range.
   errValue1 and errValue2 contain the valid lower and upper range boundaries. */
#define GPFS_FCNTL_ERR_MAXDATA_REPLICAS_RANGE     4

/* An attempt to change maxMetadataReplicas or maxDataReplicas or both
   was made on a file that is not empty. */
#define GPFS_FCNTL_ERR_FILE_NOT_EMPTY             5

/* MetadataReplicas or dataReplicas or both exceed the number of failure groups.
   errValue1 contains the maximum number of metadata failure groups.
   errValue2 contains the maximum number of data failure groups. */
#define GPFS_FCNTL_ERR_REPLICAS_EXCEED_FGMAX      6


/* This directive is used to set a file's assigned storage pool. 
   However, the directive does not cause the file data to be migrated 
   immediately. Instead the caller must append a gpfsRestripeData_t 
   directive or invoke a mmrestripefs or mmrestripefile command. 
   The caller must have root privileges to change a file's storage pool. */
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: 
                                  GPFS_FCNTL_SET_STORAGEPOOL */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int reserved;                /* unused, but should be set to 0 */
  char buffer[GPFS_FCNTL_MAX_NAME_BUFFER]; /* Null-terminated name of 
                                              storage pool to be assigned */
} gpfsSetStoragePool_t;


/* Values that may be returned by errReason */

/* Invalid storage pool name was given. */
#define GPFS_FCNTL_ERR_INVALID_STORAGE_POOL       7

/* Invalid storage pool. File cannot be assigned to given pool. */
#define GPFS_FCNTL_ERR_INVALID_STORAGE_POOL_TYPE  8

/* Invalid storage pool. Directories cannot be assigned to given pool. */
#define GPFS_FCNTL_ERR_INVALID_STORAGE_POOL_ISDIR 9

/* Invalid storage pool. System files cannot be assigned to given pool. */
#define GPFS_FCNTL_ERR_INVALID_STORAGE_POOL_ISLNK 10

/* Invalid storage pool. System files cannot be assigned to given pool. */
#define GPFS_FCNTL_ERR_INVALID_STORAGE_POOL_ISSYS 11

/* File system has not been upgraded to support storage pools */
#define GPFS_FCNTL_ERR_STORAGE_POOL_NOTENABLED    12

/* User does not have permission to perform the requested operation */
#define GPFS_FCNTL_ERR_NOPERM                     13




/* This directive is used to restripe a file's data blocks to update 
   its replication and/or migrate its data. The data movement is always 
   done immediately. */

typedef struct 
{
  long long startOffset;       /* start of range to restripe in bytes */
  long long numOfBlks;         /* blocks (size st_blksize) to restripe */
} gpfsByteRange_t;

typedef struct 
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: 
                                  GPFS_FCNTL_RESTRIPE_DATA */
  int options;                 /* options for restripe command. Defined below.
                                  See mmrestripefs command for details. */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int reserved1;               /* unused, but should be set to 0 */  
  int reserved2;               /* unused, but should be set to 0 */  
} gpfsRestripeData_t;

typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier:
                                  GPFS_FCNTL_RESTRIPE_RANGE */
  int options;                 /* options for restripe command. Defined below.
                                  See mmrestripefs command for details. */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  gpfsByteRange_t range;       /* Should be zero unless
                                  GPFS_FCNTL_RESTRIPE_RANGE_R is set */
  int reserved1;               /* unused, but should be set to 0 */
  int reserved2;               /* unused, but should be set to 0 */
} gpfsRestripeRange_t;

typedef struct {
  long long blkNum;
  long long diskNum;
  long long sector;
} BadBlock;

/* Maximum length of output buffer in gpfsRestripeRange_t */
#define MAX_COMP_BUF_LEN    (64 * 1024)
/* Maximum count of BadBlock that can be stored in output buffer */
#define MAX_COMP_BLOCK_NUM  ((MAX_COMP_BUF_LEN - sizeof(long long)) / sizeof(BadBlock))

typedef struct BadBlockInfo {
  long long nBadBlocks;         /* number of bad blocks */
  BadBlock badBlocks[0];         /* bad block info array */
} BadBlockInfo_t;

/* Difference between gpfsRestripeRange_t and gpfsRestripeRangeV2_t
   is that gpfsRestripeRangeV2_t has an outBuf which could be used
   to pass information from daemon back to caller. Currently, only
   GPFS_FCNTL_RESTRIPE_CM uses the buffer. */
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier:
                                  GPFS_FCNTL_RESTRIPE_RANGE */
  int options;                 /* options for restripe command. Defined below.
                                  See mmrestripefs command for details. */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  gpfsByteRange_t range;       /* Should be zero unless
                                  GPFS_FCNTL_RESTRIPE_RANGE_R is set */
  int reserved1;               /* unused, but should be set to 0 */
  int reserved2;               /* unused, but should be set to 0 */

  /* Fields above should be kept the same as gpfsRestripeRange_t. */

  char *outBuf;                /* buffer to store daemon returned info
                                  Caller is responsible to allocate
                                  and deallocate the buffer.
                                  Now only GPFS_FCNTL_RESTRIPE_CM use it
                                  to return bad replica info. */
  int bufLen;                  /* length of the buffer */

  int reserved3;               /* unused, but should be set to 0 */
} gpfsRestripeRangeV2_t;

/* Define values for restripe options.
   See mmrestripefs command for complete definitions. */

/* Migrate critical data off of suspended disks. */
#define GPFS_FCNTL_RESTRIPE_M   0x0001

/* Replicate data against subsequent failure. */
#define	GPFS_FCNTL_RESTRIPE_R   0x0002

/* Place file data in assigned storage pool. */
#define GPFS_FCNTL_RESTRIPE_P   0x0004

/* Rebalance file data */
#define GPFS_FCNTL_RESTRIPE_B   0x0008

/* Restripe a range of file data.  On input, the start of the range is given by
   range.startOffset (in bytes) and the size is given by range.numOfBlks (using
   a block size of st_blksize).  If range startOffset is beyond EOF (strictly
   greater than st_size for files, and inode.fileSize for directories)
   gpfs_fcntl() fails and sets errno to ERANGE.  Upon success, range.numOfBlks
   is updated to be the last block number that was processed.  Thus, the next
   iteration should set startOffset to (numOfBlks + 1) * st_blksize. */
#define GPFS_FCNTL_RESTRIPE_RANGE_R  0x0010

/* Compress or uncompress a file according to the compression bit of the file */
#define GPFS_FCNTL_RESTRIPE_C  0x0020
/* Compress or uncompress a range according to the compression bit of the file */
#define GPFS_FCNTL_RESTRIPE_RC  0x0040

/* Relocate file data */
#define GPFS_FCNTL_RESTRIPE_L 0x0080

/* Compare file data */
#define GPFS_FCNTL_RESTRIPE_CM 0x0100

/* Fast file system rebalance option */
#define GPFS_FCNTL_RESTRIPE_B_FAST 0x200

/* Compare file data but do not fix mismatch */
#define GPFS_FCNTL_RESTRIPE_CM_RO 0x0400

/* Force the restripe actions */
#define GPFS_FCNTL_RESTRIPE_FORCE 0x800

/* Values that may be returned by errReason */

/* Not enough replicas could be created because the desired degree 
   of replication is larger than the number of failure groups. */
#define GPFS_FCNTL_ERR_NO_REPLICA_GROUP          14

/* Not enough replicas could be created because there was not 
   enough space left in one of the failure groups. */
#define GPFS_FCNTL_ERR_NO_REPLICA_SPACE          15

/* There was not enough space left on one of the disks to properly 
   balance the file according to the current stripe method. */
#define GPFS_FCNTL_ERR_NO_BALANCE_SPACE          16

/* The file could not be properly balanced because one or more 
   disks are unavailable. */
#define GPFS_FCNTL_ERR_NO_BALANCE_AVAILABLE      17

/* All replicas were on disks that have since been deleted 
   from the stripe group. */
#define GPFS_FCNTL_ERR_ADDR_BROKEN               18

/* No immutable attribute can be set on directories */
#define GPFS_FCNTL_ERR_NO_IMMUTABLE_DIR          19

/* No immutable attribute can be set on system files */
#define GPFS_FCNTL_ERR_NO_IMMUTABLE_SYSFILE      20

/* Immutable and indefinite retention flag wrong */
#define GPFS_FCNTL_ERR_IMMUTABLE_FLAG            21

/* Immutable and indefinite retension flag wrong */
#define GPFS_FCNTL_ERR_IMMUTABLE_PERM            22

/* AppendOnly flag should be set separately */
#define GPFS_FCNTL_ERR_APPENDONLY_CONFLICT       23

/* Cannot set immutable or appendOnly on snapshots */
#define GPFS_FCNTL_ERR_NOIMMUTABLE_ONSNAP        24

/* An attempt to change maxDataReplicas or maxMetadataReplicas 
   was made on a file that has extended attributes. */
#define GPFS_FCNTL_ERR_FILE_HAS_XATTRS           25

/* This file is not part of a GPFS file system */
#define GPFS_FCNTL_ERR_NOT_GPFS_FILE             26






/* Values that may be returned by errValue1 */

/* Strict replica allocation option set to be yes */
#define GPFS_FCNTL_STATUS_STRICT_REPLICA_YES           0x0010

/* Strict replica allocation option set to be no */
#define GPFS_FCNTL_STATUS_STRICT_REPLICA_NO            0x0020

/* Strict replica allocation option set to be whenpossible */
#define GPFS_FCNTL_STATUS_STRICT_REPLICA_WHENPOSSIBLE  0x0040

/* Structures for specifying the various gpfs_fcntl inquiries.
   The inquiry directives may be used to obtain attributes of a file
   such as the file's replication factors, storage pool name, 
   fileset name or snapshot name. */

/* This inquiry is used to obtain a file's replication factors. */
typedef struct
 {
   int structLen;               /* length of this structure */
   int structType;              /* inquiry identifier: 
                                   GPFS_FCNTL_GET_REPLICATION */
   int metadataReplicas;        /* returns the current number of copies 
                                   of indirect blocks for the file. */
   int maxMetadataReplicas;     /* returns the maximum number of copies
                                   of indirect blocks for the file. */
   int dataReplicas;            /* returns the current number of copies
                                   of data blocks for the file. Note
                                   that perfReplicas is not included here. */
   int maxDataReplicas;         /* returns the maximum number of copies
                                   of data blocks for the file. */
   int status;                  /* returns the status of the file.
                                   Status values defined below. */
   int perfReplicas;            /* returns the number of performance copies */
} gpfsGetReplication_t;


/* Flag definitions */

/* If set this file may have some broken data block. */
#define GPFS_FCNTL_STATUS_BROKEN         0x80000000

/* If set this file may have some data where the only replicas are 
   on suspended disks; implies some data may be lost if suspended
   disks are removed. */
#define GPFS_FCNTL_STATUS_EXPOSED        0x40000000

/* If set this file may not be properly replicated, i.e. some data 
   may have fewer or more than the desired number of replicas, 
   or some replicas may be on suspended disks. */
#define GPFS_FCNTL_STATUS_ILLREPLICATED  0x20000000

/* If set this file may not be properly balanced. */
#define GPFS_FCNTL_STATUS_UNBALANCED     0x10000000

/* If set this file has stale data blocks on at least one of the disks
   that are marked as unavailable or recovering in the stripe group 
   descriptor. */
#define GPFS_FCNTL_STATUS_DATAUPDATEMISS 0x08000000

/* If set this file has stale indirect blocks on at least one 
   unavailable or recovering disk. */
#define GPFS_FCNTL_STATUS_METAUPDATEMISS 0x04000000

/* If set this file may not be properly placed, i.e. some data may 
   be stored in an incorrect storage pool */
#define GPFS_FCNTL_STATUS_ILLPLACED      0x02000000
#define GPFS_FCNTL_STATUS_ILLCOMPRESSED  0x01000000

#define GPFS_FCNTL_STATUS_FPO_ILLPLACED  0X00800000



/* This inquiry is used to obtain the name of the storage pool in which
   the file's data is stored. The size of the buffer may vary, but it must be 
   a multiple of 8. Upon successful completion of the call, the buffer 
   will contain a null-terminated character string for the name of the
   file's storage pool. */
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* inquiry identifier: 
                                   GPFS_FCNTL_GET_STORAGEPOOL */
  char buffer[GPFS_FCNTL_MAX_NAME_BUFFER]; /* returns the file's
                                              storage pool name */
} gpfsGetStoragePool_t;


/* This inquiry is used to obtain the name of the fileset to which this 
   file has been assigned. The size of the buffer may vary, but it must be 
   a multiple of 8. Upon successful completion of the call, the buffer 
   will contain a null-terminated character string for the name of the
   file's fileset. */
typedef struct
{
   int structLen;               /* length of this structure */
   int structType;              /* inquiry identifier: 
                                   GPFS_FCNTL_GET_FILESETNAME */
  char buffer[GPFS_FCNTL_MAX_NAME_BUFFER]; /* returns with the file's
                                              fileset name */
} gpfsGetFilesetName_t;


/* This inquiry is used to obtain the name of the snapshot that includes 
   this file. If the file is not part of a snapshot, then a zero-length
   string will be returned. The size of the buffer may vary, but it must be 
   a multiple of 8. Upon successful completion of the call, the buffer 
   will contain a null-terminated character string for the name of the
   snapshot that includes this file. */
typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* inquiry identifier: 
                                   GPFS_FCNTL_GET_SNAPSHOTNAME */
  char buffer[GPFS_FCNTL_MAX_NAME_BUFFER]; /* returns with the file's
                                              snapshot name */
} gpfsGetSnapshotName_t;

/* Allow tschattr to change file immutable attribute */
#define GPFS_FCNTL_SET_IMMUTABLE   5000

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                   GPFS_FCNTL_SET_IMMUTABLE */
  int setImmutable;            /* value to set the immutable flag */
  int setIndefiniteRetention;  /* value to set IndefiniteRetention */
  int noCtime;                 /* allow ctime change or not upon attribute change*/
  int errReasonCode;           /* reason code                   */
} gpfsSetImmutable_t;

#define GPFS_FCNTL_GET_IMMUTABLE   5001

typedef struct {
   int structLen;               /* length of this structure */
   int structType;              /* function identifier: 
                                   GPFS_FCNTL_GET_IMMUTABLE */
   int immutable;               /* value of the immutable flag */
   int indefiniteRetention;     /* value of the indefiniteRetention flag */
   int errReasonCode;           /* reason code                   */
   int reserved;                /* reserved field */
} gpfsGetImmutable_t;

#define GPFS_FCNTL_SET_EXPIRATION_TIME   5002

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                   GPFS_FCNTL_SET_IMMUTABLE */
  long long expTime;           /* expiration time               */
  int errReasonCode;           /* reason code                   */
  int reserved;                /* reserved field */
} gpfsSetExpTime_t;

#define GPFS_FCNTL_GET_EXPIRATION_TIME   5003

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                   GPFS_FCNTL_GET_EXPIRATION_TIME */
  long long expTime;           /* expiration Time          */
  int errReasonCode;           /* reason code                   */
  int reserved;                /* reserved field */
} gpfsGetExpTime_t;


#define GPFS_FCNTL_SET_APPENDONLY   5004

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                   GPFS_FCNTL_SET_APPENDONLY */
  int setAppendOnly;           /* value to set the appendOnly flag */
  int setIndefiniteRetention;  /* value to set IndefiniteRetention */
  int errReasonCode;           /* reason code                   */
  int noCtime;                 /* allow ctime change or not upon attribute change*/
} gpfsSetAppendOnly_t;

#define GPFS_FCNTL_GET_APPENDONLY  5005

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                  GPFS_FCNTL_GET_APPENDONLY */
  int appendOnly;              /* value of the appendOnly flag */
  int indefiniteRetention;     /* value of the indefiniteRetention flag */
  int errReasonCode;           /* reason code                   */
  int reserved;                /* reserved field */
} gpfsGetAppendOnly_t;


/* Deprecated in favor of calling gpfs_prealloc(fd, 0, 0). */
#define GPFS_FCNTL_COMPACT_DIR  5006

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                  GPFS_FCNTL_COMPACT_DIR */
  int errReasonCode;           /* reason code */
  int reserved;                /* reserved field */
} gpfsCompactDir_t;



/* 
 * Provide access to a file's extended attributes
 *
 * Extended attributes are persistent name,value pairs assigned to files or
 * directories. The name is typically a character string which is terminated 
 * by a null ('\0') character which is included in the name length. 
 * Attributes set internally or for dmapi may follow other conventions.
 * An attributes's value is a binary string which is not interpreted
 * by this api.
 * 
 * Attribute names are typically divided into namespaces, both to avoid
 * collisions and to enforce access controls. The namespaces currently
 * defined are:
 *
 * "user."     -- requires permission to access file data
 * "system."   -- used by kernel for access control lists
 * "trusted."  -- requires admin/root privilege
 * "security." -- used by Security Enhanced Linux
 * "archive."  -- reserved for GPFS
 * "dmapi."    -- reserved for the X/Open Data Storage Management API (XDSM)
 * "gpfs."     -- reserved for GPFS
 *
 * For example, "user.name1" or "system.posix_acl_default". 
 * Additional namespaces may be defined in future releases. Users should
 * restrict the names used to a predefined namespace.
 *
 * Setting or resetting attributes reserved by GPFS or other middleware 
 * services may have unintended consequences and is not recommended.
 *
 * Attributes are created via the SET_XATTR call with a value length of
 * zero or more bytes. By default, setting an extended attribute will create
 * the attribute if it does not exist or will replace the current value if 
 * it does. Using the CREATE flag causes a pure create which fails if the 
 * attribute already exists. Likewise using the REPLACE flag causes a pure
 * replace which fails if the attribute does not exist. 
 *
 * Attributes are deleted by calling SET_XATTR with a negative length. Using
 * the DELETE flag causes a pure delete which fails if the attribute does 
 * not exist.
 * 
 * By default, setting or deleting an extended attribute is done 
 * asynchronously. That is to say, the update is not written to disk until
 * some time after the call completes. Using the SYNC option causes the
 * update to be committed to disk before the call completes. The update 
 * to a single attribute is always done atomically. Either the original value
 * is retained or the complete new value will be set. 
 *
 * More than one attribute may be set or deleted within a single fcntl call.
 * The updates are applied in the order specified in the fcntl vector.
 * If the file system fails, there is no guarantee that all updates will
 * be applied, even if using the SYNC options. If sync is specified on the 
 * last update in the vector, then all prior updates to attributes will also
 * be written with the final update.
 *
 * In order to see an attribute via the GET_XATTR or LIST_XATTR call
 * the caller must have sufficient privilege to set the attribute. 
 *
 * Errors are returned in the errReturnCode field.
 * 
 * 
 * Note: Extended attributes are stored as character or binary strings.
 * Care should be taken when used on mixed architectures with different
 * byte orders or word sizes to insure the attribute values are in an 
 * architecture independent format. 
 *
 */
#define GPFS_FCNTL_GET_XATTR  6001
#define GPFS_FCNTL_SET_XATTR  6002
#define GPFS_FCNTL_LIST_XATTR 6003

#define GPFS_FCNTL_XATTR_MAX_NAMELEN   256 /* includes trailing null char */
#define GPFS_FCNTL_XATTR_MAX_VALUELEN (16 * 1024) 

#define GPFS_FCNTL_XATTRFLAG_NONE      0x0000
#define GPFS_FCNTL_XATTRFLAG_SYNC      0x0001 /* synchronous update
                                                 All updates are committed
                                                 before the call returns */
#define GPFS_FCNTL_XATTRFLAG_CREATE    0x0002 /* pure create
                                                 will fail if already exists */
#define GPFS_FCNTL_XATTRFLAG_REPLACE   0x0004 /* pure replace
                                                 will fail if does not exist */
#define GPFS_FCNTL_XATTRFLAG_DELETE    0x0008 /* pure delete
                                                 will fail if does not exist */
#define GPFS_FCNTL_XATTRFLAG_NO_CTIME  0x0010 /* Update will not set ctime.
                                                 Must have admin authority. */
#define GPFS_FCNTL_XATTRFLAG_MO_PARENT 0x0020 /* Update clone parent.
                                                 Must have admin authority. */
#define GPFS_FCNTL_XATTRFLAG_RESERVED  0x8000 /* obsolete */

/* Define error reason codes for extended attributes */
#define GPFS_FCNTL_ERR_NO_ATTR             27
#define GPFS_FCNTL_ERR_ATTR_EXISTS         28
#define GPFS_FCNTL_ERR_BUFFER_TOO_SMALL    29
#define GPFS_FCNTL_ERR_NO_ATTR_SPACE       30
#define GPFS_FCNTL_ERR_INVAL_VALUE         31
#define GPFS_FCNTL_ERR_INVAL_READ_REPLICA_RULE  32


typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                  GPFS_FCNTL_GET_XATTR 
                                  or GPFS_FCNTL_SET_XATTR */
  int nameLen;                 /* length of attribute name
                                  may include trailing '\0' character */
  int bufferLen;                /* 
                                  for GPFS_FCNTL_GET_XATTR
                                    INPUT: length of buffer
                                    OUTPUT: length of attribute value
 
                                  for GPFS_FCNTL_SET_XATTR
                                    INPUT: length of attribute value
                                           length = -1 to delete attribute */
  unsigned int flags;          /* defined above. */
  int errReasonCode;           /* reason code */
  char buffer[0];              /* buffer for name and value.
                                  
                                  for GPFS_FCNTL_GET_XATTR

                                    INPUT: name begins at offset 0 
                                           and must be null terminated.
                                    OUTPUT: name is returned unchanged
                                            value begins at nameLen rounded up
                                            to a multiple of 8.

                                  for GPFS_FCNTL_SET_XATTR

                                    INPUT: name begins at offset 0 
                                           and must be null terminated.
                                           value begins at nameLen rounded up
                                           to a multiple of 8.

                                  actual length of buffer should be 
                                  nameLen rounded up to a multiple of 8 
                                  + valueLen rounded up to a multiple of 8 

                                  Buffer size set by caller. Maximum
                                  size is (GPFS_FCNTL_XATTR_MAX_NAMELEN +
                                  GPFS_FCNTL_XATTR_MAX_VALUELEN) */
} gpfsGetSetXAttr_t;

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                  GPFS_FCNTL_LIST_XATTR */
  int bufferLen;               /* INPUT: length of buffer
                                  OUTPUT: length of returned list of names */
  int errReasonCode;           /* reason code */
  char buffer[0];
                               /* buffer for returned list of names
                                  Each attribute name is prefixed with
                                  a 1-byte name length. The next attribute name
                                  follows immediately in the buffer (and is
                                  prefixed with its own length). Following
                                  the last name a '\0' is appended to
                                  terminate the list. The returned bufferLen
                                  includes the final '\0'.
                                  
                                  \6abcdef\3ABC\9user.name\0

				  Caution: An attribute name may include embedded 
				  and/or trailing null bytes:
				       
				  \13gpfs.DIRECTIO\11dmapi.t\1\0\3\0\0
                                  
                                  The actual length of the buffer required 
                                  depends on the number of attributes set on
                                  the file and the length of each attribute name.
                                  If the buffer provided is too small for all
                                  of the returned names, the errReasonCode
                                  will be set to GPFS_FCNTL_ERR_BUFFER_TOO_SMALL
                                  and bufferLen will be set to the minimum
                                  size buffer required to list all attributes.
                                  An initial buffer length of 0 may be used to
                                  query the attributes and determine the 
                                  correct buffer size for this file. */

} gpfsListXAttr_t;






typedef struct OffsetLoc {
  long long offset;
  int diskNum[];      /* array of locations based on number of replicas returned */
} OffsetLoc;

typedef struct FilemapIn {
  long long startOffset; /* start offset in bytes */
  long long skipfactor;  /* number of bytes to skip before next offset read */
  long long length;      /* number of bytes (start offset + length / skipfactor = numblks returned */
  int mreplicas;         /* number of replicas user wants. 0 - all,
                            1 - primary, 2 - primary and 1st replica, 3 - all */
  int reserved;          /* for now to align it to the 8byte boundary */
} FilemapIn;

typedef struct FilemapOut {
  int numReplicasReturned;
  int numBlksReturned;
  int blockSize;
  int blockSizeHigh;           /* High 32bits of block size */
  /* packs offset, disklocation1, disklocation2.... */
  char buffer [GPFS_MAX_FCNTL_LENGTH-1024];
} FilemapOut;

typedef struct GetDataBlkDiskIdx {
  int structLen;         /* length of this structure */
  int structType;        /* function identifier: GPFS_FCNTL_GET_DATABLKDISKIDX */
  FilemapIn filemapIn;   /* Input parameters specified by the user */
  FilemapOut filemapOut; /* Output data */
} GetDataBlkDiskIdx;
typedef struct GetDataBlkDiskIdx gpfsGetDataBlkDiskIdx_t;

/* This is same size as FilemapIn for use by
   FileMetadata:::getDataBlockDiskNums() */
typedef struct DataBlockDiskNumsIn {
  long long startDataBlockIndex;
  long long unused[3];
} DataBlockDiskNumsIn;

/* This is same size as FilemapOut but for use by
   FileMetadata:::getDataBlockDiskNums() */
typedef struct DataBlockDiskNumsOut {
  long long nextDataBlockIndex;
  unsigned char maxDataReplicas;
  unsigned char bDataInInode;
  unsigned char bMetanode;
  unsigned char bEndOfFile;
  int nArrEntries;

  /* The output buffer is an array of unsigned short ints grouped into replica
     replica sets. Each replica set contains -
       replica1diskNum, replica2diskNum, ..., upto inode.maxDataReplicas.
     Consecutive replica sets represent diskNums for consecutive data blocks.
     Each unsigned short array entry has its low 13 bits reserved for disk
     number and its high 3 bits reserved for flags (see below). */

# define DISK_NUM_ENTRY_DISK_NUM_MASK             0x1FFF
# define DISK_NUM_ENTRY_FLAG_NONE                 0x0000
  /* this replica is the reference */
# define DISK_NUM_ENTRY_FLAG_REF_REPLICA          0x2000
  /* an invalid replica reference was specified */
# define DISK_NUM_ENTRY_FLAG_REF_REPLICA_ERR      0x4000
  /* this replica DA is invalid */
# define DISK_NUM_ENTRY_FLAG_INVALID_DA           0x6000
  /* this and the next 4 entries in DataBlockDiskNumsOut.arr make up the 61-bit
     data block index of the next replica set in big endian format */
# define DISK_NUM_ENTRY_FLAG_DATA_BLOCK_INDEX     0x8000

  unsigned short arr[(GPFS_MAX_FCNTL_LENGTH - 1024)/sizeof(unsigned short)];
} DataBlockDiskNumsOut;

/* This is same size as GetDataBlkDiskIdx but for use by
   FileMetadata:::getDataBlockDiskNums() */
typedef struct DataBlockDiskNums {
  int structLen; /* length of this structure */
  int structType; /* function identifier: GPFS_FCNTL_GET_DATA_BLOCK_DISK_NUMS */
  DataBlockDiskNumsIn diskNumsIn; /* Input parameters specified by the user */
  DataBlockDiskNumsOut diskNumsOut; /* Output data */
} DataBlockDiskNums;
typedef struct DataBlockDiskNums gpfsDataBlockDiskNums_t;

typedef struct {
  int structLen;               /* length of this structure */
  int structType;              /* function identifier: 
                                  GPFS_FCNTL_FSYNC_RANGE */
  int how;
  long long startOffset;
  long long length;
} gpfsFsyncRange_t;

#define GPFS_FCNTL_ERR_COMPRESS_NOTREGULAR         32
#define GPFS_FCNTL_ERR_COMPRESS_CLONE              33
#define GPFS_FCNTL_ERR_COMPRESS_SNAPSHOT           34
#define GPFS_FCNTL_ERR_COMPRESS_OLD_FILESYSTEM     35
#define GPFS_FCNTL_ERR_COMPRESS_AFM                36
#define GPFS_FCNTL_ERR_COMPRESS_HYPERALLOC         37
#define GPFS_FCNTL_ERR_COMPRESS_NOT_ALLOWED        38

#define GPFS_FCNTL_ERR_COMPARE_DATA_IN_INODE       39
#define GPFS_FCNTL_ERR_COMPRESS_LIB_SELECT         40
#define GPFS_FCNTL_ERR_COMPRESS_LIB_SUPPORT        41
#define GPFS_FCNTL_ERR_COMPRESS_LIB_LOAD_FAILED    42
#define GPFS_FCNTL_ERR_DISK_OFFLINE                43
#define GPFS_FCNTL_WARN_FS_QUIESCING               44

/* A file in a performance pool can only have 1 replica. */
#define GPFS_FCNTL_ERR_PERF_POOL_ONE_REPLICA       45

/* The number of performance replicas for a file is out of range. */
#define GPFS_FCNTL_ERR_PERF_REPLICAS_RANGE         46

/* The replication values are not supported by file system version. */
#define GPFS_FCNTL_ERR_REPL_OLD_FILESYSTEM         47

/* There is no performance pool to support a performance replica. */
#define GPFS_FCNTL_ERR_NO_PERFORMANCE_POOL         48

/* Total number of data replicas exceeds maximum number of data replicas */
#define GPFS_FCNTL_ERR_TOTAL_REPLICAS_RANGE        49

/* Target pool is performane pool but incorrect dataReplicas and/or perfReplicas */
#define GPFS_FCNTL_ERR_PERFPOOL_REPLICAS_RANGE     50

/* Turn on or off compression. Zlib compression library is selected by default when using
   this GPFS_FCNTL_COMPRESSION_ON structType/directive. Use gpfsSetCompressionLib_t,
   with GPFS_FCNTL_COMPRESSION_LIB directive (defined below) to select any other supported
   compression libraries (lz4, zfast, alphae and alphah are currently supported). This directive 
   only flags the file to be compressed or uncompressed (and it turns on the file's 
   illCompressed bit). To actually compress or uncompressed the file, append a gpfsRestripeData_t 
   or gpfsRestripeRange_t structure with GPFS_FCNTL_RESTRIPE_DATA or GPFS_FCNTL_RESTRIPE_RANGE
   structType respectively and set the GPFS_FCNTL_RESTRIPE_C bit in its options field. */
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: GPFS_FCNTL_COMPRESSION_ON
                                  GPFS_FCNTL_COMPRESSION_OFF */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int reserved;                /* unused, but should be set to 0 */
} gpfsSetCompression_t;

/* Specify the compression library to be used to compress a file.
   Supported compression libraries include "z", "lz4", "zfast" (for zlib, lz4 and zlib-fast general 
   purpose compression libraries) and "alphae", "alphah" (special purpose, FASTQ genomic data
   compression libraries). This directive only sets the selected compression library of the file
   without actually compressing the file.  To unset selected compression library of the
   file, use gpfsCsetCompression_t defined above with GPFS_FCNTL_COMPRESSION_OFF as
   structType.  To actually compress or uncompress the file, append a gpfsRestripeData_t
   or gpfsRestripeRange_t structure with GPFS_FCNTL_RESTRIPE_DATA or GPFS_FCNTL_RESTRIPE_RANGE
   as the structType respectively and set the GPFS_FCNTL_RESTRIPE_C bit in its options field. */
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: GPFS_FCNTL_COMPRESSION_LIB */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int reserved;                /* unused, but should be set to 0 */
  char *libName;               /* Null-terminated name of compression lib to be used */
} gpfsSetCompressionLib_t;

/* fle compression status bits */
#define GPFS_FCNTL_COMPRESS_UNSET           0x0
#define GPFS_FCNTL_COMPRESS_SET             0x1
#define GPFS_FCNTL_COMPRESS_ILL_COMPRESSED  0x2

typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: GPFS_FCNTL_GET_COMPRESSION */
  int compressStatus;          /* file compression status bits defined above */
  int errReason;               /* returned reason request failed.
                                  Defined below. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int reserved1;               /* unused, but should be set to 0 */
  int reserved2;               /* unused, but should be set to 0 */
} gpfsGetCompression_t;

/* compression non-support bits */
#define GPFS_FCNTL_CANNOT_COMPRESS_OLD_FILESYSTEM     0x1
#define GPFS_FCNTL_CANNOT_COMPRESS_SNAPSHOT           0x2
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: GPFS_FCNTL_GET_COMPRESSION_SUPPORT */
  int compressNonSupport;      /* compression support status of this object or directory tree
                                  0: compression is supported.  The non-support bit(s)
                                  defined above indicates the reason for non-support otherwise. */
  int errReason;               /* returned reason request failed. */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int reserved1;               /* unused, but should be set to 0 */
  int reserved2;               /* unused, but should be set to 0 */
} gpfsGetCompressionSupport_t;

/* reason if snapshot migration is unsupported */
#define GPFS_FCNTL_CANNOT_MIGRATE_SNAP_OLD_FILESYSTEM     0x1
#define GPFS_FCNTL_CANNOT_MIGRATE_SNAP_OLD_SNAPSHOT       0x2
typedef struct
{
  int structLen;               /* length of this structure */
  int structType;              /* directive identifier: GPFS_FCNTL_GET_SNAP_MIGRATION_SUPPORT */
  int supported;               /* migration support status of this snapshot 
                                  1/0 - supported/unsupported
                                  if unsupported, unsupportRC contains the reason */
  int unsupportRC;             /* reason why unsupported */
  int errReason;               /* returned reason request failed */
  int errValue1;               /* returned value depending upon errReason */
  int errValue2;               /* returned value depending upon errReason */
  int reserved1;               /* unused, but should be set to 0 */
} gpfsGetSnapMigrationSupport_t;

/* NAME:        gpfs_fcntl()
 *
 * FUNCTION:    Pass hints and directives to GPFS on behalf of an open file
 *
 * Returns:      0      Successful
 *              -1      Failure
 *
 * Errno:       ENOSYS  Function not available
 *              EBADF   Bad file handle
 *              EINVAL  Not a GPFS file
 *              EINVAL  Not a regular file
 *              EINVAL  Ill-formed hint or directive
 *              E2BIG   Argument longer than GPFS_MAX_FCNTL_LENGTH
 */

int GPFS_API
gpfs_fcntl(gpfs_file_t fileDesc,   /* Open file descriptor */
           void* fcntlArgP);       /* argument list */

/*
 * NAME:        gpfs_restripe_file()
 *
 * FUNCTION:    Restripes a file by calling gpfs_fcntl for noBlocks
 *              from start offset. If noBlocks 0 ==> use default increment size
 *
 * Returns:      0      Successful
 *              -1      Failure
 *
 * Errno:       ENOSYS  No quality of service function available
 *              ENOENT  File not found
 *              EINVAL  Not a GPFS file
 *              ESTALE  cached fs information was invalid
 */

int GPFS_API
gpfs_restripe_file(gpfs_file_t fileDesc, /* Open file descriptor */
                   void* fcntlArgP,      /* argument list */
                   int noBlocks);        /* number of blocks used for each
                                            iteration when restriping a
                                            file */
/* similar but by logical part number */
int GPFS_API
gpfs_restripe_file_by_parts(gpfs_file_t fileDesc,
			    void* fcntlArgP,
			    int part_number,      /* e.g. part 3 of ... */
			    int number_of_parts); /* ... 4 parts */

/* gpfs_ifile_t versions of above. */

int GPFS_API
gpfs_restripe_ifile(gpfs_ifile_t* ifileP, /* Open file descriptor */
                    void* fcntlArgP,      /* argument list */
                    int noBlocks);        /* number of blocks used for each
                                             iteration when restriping a
                                             file */

int GPFS_API
gpfs_restripe_ifile_by_parts(gpfs_ifile_t* ifileP,
                             void* fcntlArgP,
                             int part_number,      /* e.g. part 3 of ... */
                             int number_of_parts); /* ... 4 parts */

#ifdef __cplusplus
}
#endif

#endif /* _h_gpfs_fcntl */
