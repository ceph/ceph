/* 
This file, journal2.h, features structures which are used for the each of the
logging systems used in the lsvd system, including structures of superblocks for
both the write and read caches, and the connecting structures between them and 
the how they fit together with rados and s3
*/
#ifndef JOURNAL2_H
#define JOURNAL2_H

#include <stdint.h>
#include <vector>
#include <uuid/uuid.h>

struct j_extent {
    uint64_t lba : 40;		// volume LBA (in sectors)
    uint64_t len : 24;		// length (sectors)
} __attribute__((packed));

// could do a horrible hack to make this 14 bytes, but not worth it
struct j_map_extent {
    uint64_t lba : 40;		// volume LBA (in sectors)
    uint64_t len : 24;		// length (sectors)
    uint64_t plba;		// on-SSD LBA
} __attribute__((packed));

struct j_length {
    int32_t page;		// in pages
    int32_t len;		// in pages
    bool operator<(const j_length &other) const { // for sorting
        return page < other.page;
    }
} __attribute__((packed));

enum {LSVD_J_DATA    = 10,
      LSVD_J_CKPT    = 11,
      LSVD_J_PAD     = 12,
      LSVD_J_SUPER   = 13,
      LSVD_J_W_SUPER = 14,
      LSVD_J_R_SUPER = 15};

/* for now we'll assume that all entries are contiguous
 */
struct j_hdr {
    uint32_t magic;
    uint32_t type;		// LSVD_J_DATA
    uint32_t version;		// 1
    int32_t  len;		// in 4KB blocks, including header
    uint64_t seq;
    uint32_t crc32;		// TODO: implement this
    int32_t  extent_offset;	// in bytes
    int32_t  extent_len;        // in bytes
    int32_t  prev;              // reverse link for recovery
} __attribute__((packed));

/* probably in the second 4KB block of the parition
 * this gets overwritten every time we re-write the map. We assume the 4KB write is
 * atomic, and write out the new map before updating the superblock.
 */
struct j_write_super {
    uint32_t magic;
    uint32_t type;		// LSVD_J_W_SUPER
    uint32_t version;		// 1

    uint32_t clean;
    uint64_t seq;		// next write sequence (if clean)

    /* all values are in 4KB block units. */
    
    /* Map and length checkpoints live in this region. Allocation within this
     * range is arbitrary, just set {map/len}_{start/blocks/entries} properly
     */
    int32_t meta_base;
    int32_t meta_limit;
    
    /* FIFO range is [base,limit), 
     *  valid range accounting for wraparound is [oldest,next)
     *  'wrapped' 
     */
    int32_t base;
    int32_t limit;
    int32_t next;
    
    int32_t map_start;		// type: j_map_extent
    int32_t map_blocks;
    int32_t map_entries;

    int32_t len_start;	// type: j_length
    int32_t len_blocks;
    int32_t len_entries;
} __attribute__((packed));

/* probably in the third 4KB block, never gets overwritten (overwrite map in place)
 * uses a fixed map with 1 entry per 64KB block
 * to update atomically:
 * - reclaim batch of blocks, then write map. (free entries: obj=0)
 * - allocate blocks, then write map
 * - recover free list to memory on startup
 */
struct j_read_super {
    uint32_t magic;
    uint32_t type;		// LSVD_J_R_SUPER
    uint32_t version;		// 1

    int32_t unit_size;		// cache unit size, in sectors

    /* note that the cache is not necessarily unit-aligned
     */
    int32_t base;		// the cache itself
    int32_t units;		// length, in @unit_size segments

    /* each has @cache_segments entries
     */
    int32_t map_start;		// extmap::obj_offset
    int32_t map_blocks;
} __attribute__((packed));

      
/* this goes in the first 4KB block in the cache partition, and never
 * gets modified
 */
struct j_super {
    uint32_t magic;
    uint32_t type;		// LSVD_J_SUPER
    uint32_t version;		// 1

    /* both are single blocks, so we only need a block number
     */
    uint32_t write_super;
    uint32_t read_super;

    uuid_t   vol_uuid;
} __attribute__((packed));

#endif
