// experimental GPFS RDMA APIs
#pragma once

#include "gpfs.h"

typedef unsigned long long gpfs_size64_t;
typedef long long gpfs_ssize64_t;
typedef unsigned short gpfs_uint16_t;
typedef int gpfs_int32_t;
typedef unsigned int gpfs_uint32_t;
typedef unsigned long long gpfs_uint64_t;

/* GPFS RDMA info type (gpfs_rdma_info_type field in gpfs_rdma_info_t) */
#define GPFS_RDMA_INFO_TYPE_DC 1

/* Select any virtual fabric (fab_num field in gpfs_rdma_info_dc_t) */
#define GPFS_RDMA_FABRIC_ANY (-1)

/* GPFS RDMA info structure for a RDMA transfer from/to a remote memory buffer
   via a DC queue pair */
typedef struct gpfs_rdma_info_dc
{
    gpfs_uint16_t lid;       /* Subnet local identifier of the remote host port */
    gpfs_uint32_t qp_num;    /* QP number of DCT on the remote host */

    /* Remote memory buffer used to set up RDMA on NSD server */
    gpfs_uint64_t rem_vaddr; /* Address */
    gpfs_uint32_t rkey;      /* rkey */

    /* Client information used to set up RDMA on NSD server */
    gpfs_uint64_t gid[2];    /* 16-byte global identifier of the client node port */
    gpfs_uint32_t dc_key;    /* 32-bit DC key */
    gpfs_int32_t fab_num;    /* Virtual fabric number for RDMA port selection on NSD server */
} gpfs_rdma_info_dc_t;

/* Generic structure for RDMA transfer from/to remote memory buffers */
typedef struct gpfs_rdma_info
{
    int gpfs_rdma_info_type;
    union
    {
        gpfs_rdma_info_dc_t rdma_info_dc;
    };
} gpfs_rdma_info_t;


/* NAME:        gpfs_rdma_shadow_buffer_size()
 *
 * FUNCTION:    Set the size of the RDMA shadow buffer. The default size is
 *              32 MB.
 *
 * Input:       new_size: Requested new RDMA shadow buffer size in bytes.
 *
 * Returns:     The new size of the RDMA shadow buffer. The shadow buffer
 *              size can be set only once. If the shadow buffer size is
 *              already set, the current size will be returned.
 *              On error -1 is returned.
 *
 * Errno:       ENOMEM  Memory allocation for RDMA shadow buffer failed.
 *
 */
int GPFS_API
gpfs_rdma_shadow_buffer_size(gpfs_size64_t new_size);


/* NAME:        gpfs_rdma_pread()
 *
 * FUNCTION:    Perform a file read operation at a given offset and move file data via RDMA to a remote memory
 *              buffer.
 *
 *              If the read operation qualifies for direct I/O the file data is moved via RDMA to the remote
 *              memory buffer specified via gpfs_rdma_info and errno is set to EOK.
 *
 *              If RDMA is not possible errno is set to EOPNOTSUPP. In this case the read operation should be
 *              retried via a pread() system call.
 *
 * Input:       fileDesc:       File descriptor
 *              count:          Number of bytes to read from file
 *              offset:         Offset within file to read from
 *              gpfs_rdma_info: Pointer to RDMA information structure
 *
 * Returns:     On success, the number of bytes read is returned.
 *              On error -1 is returned.
 *
 * Errno:       EOK         File read operation and file data move via RDMA was successful.
 *              EOPNOTSUPP  File data movement via RDMA not possible.
 *              EINVAL      Invalid RDMA info structure provided.
 *              any other error code documented for pread()
 */
gpfs_ssize64_t GPFS_API
gpfs_rdma_pread(gpfs_file_t fd,
                gpfs_size64_t count,
                gpfs_off64_t offset,
                const gpfs_rdma_info_t *gpfs_rdma_info);


/* NAME:        gpfs_rdma_pwrite()
 *
 * FUNCTION:    Perform a file write operation at a given offset and get file data via RDMA from a remote memory
 *              buffer.
 *
 *              If the write operation qualifies for direct I/O the file data to be written is moved via RDMA
 *              from the remote memory buffer specified via gpfs_rdma_info and errno is set to EOK.
 *
 *              If RDMA is not possible errno is set to EOPNOTSUPP. In this case the write operation should be
 *              retried via a pwrite() system call.
 *
 * Input:       fileDesc:       File descriptor
 *              count:          Number of bytes to write to file
 *              offset:         Offset within file to write to
 *              gpfs_rdma_info: Pointer to RDMA information structure
 *
 * Returns:     On success, the number of successfully written bytes read is returned.
 *              On error -1 is returned.
 *
 * Errno:       EOK         File data move via RDMA and file write operation was successful.
 *              EOPNOTSUPP  File data movement via RDMA not possible.
 *              EINVAL      Invalid RDMA info structure provided.
 *              any other error code documented for pwrite()
 */
gpfs_ssize64_t GPFS_API
gpfs_rdma_pwrite(gpfs_file_t fd,
                 gpfs_size64_t count,
                 gpfs_off64_t offset,
                 const gpfs_rdma_info_t *gpfs_rdma_info);

