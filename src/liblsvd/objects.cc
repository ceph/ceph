/*
 * file:        objects.cc
 * description: serializers / deserializers for object format
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#include <sys/uio.h>
#include <string.h>
#include <zlib.h>

#include "lsvd_types.h"
#include "backend.h"
#include "objects.h"

extern void do_log(const char *fmt, ...);

char *object_reader::read_object_hdr(const char *name, bool fast) {
    obj_hdr *h = (obj_hdr*)malloc(4096);
    iovec iov = {(char*)h, 4096};
    int rv;
    if ((rv = objstore->read_object(name, &iov, 1, 0)) < 0)
	goto fail;
    if (fast)
	return (char*)h;
    if (h->hdr_sectors > 8) {
	size_t len = h->hdr_sectors * 512;
	h = (obj_hdr*)realloc(h, len);
	iovec iov = {(char*)h, len};
	if (objstore->read_object(name, &iov, 1, 0) < 0)
	    goto fail;
    }
    return (char*)h;
fail:
    free((char*)h);
    return NULL;
}

/* read all info from superblock, returns a vast number of things:
 * [super, vol_size] = f(name, &ckpts, &clones, *&snaps):
 *  - super - pointer to buffer (must be freed)
 *  - vol_size - in bytes (-1 on failure)
 *  - ckpts, clones, snaps - what you'd expect
 */
std::pair<char*,ssize_t>
object_reader::read_super(const char *name, std::vector<uint32_t> &ckpts,
			  std::vector<clone_info*> &clones,
			  std::vector<snap_info*> &snaps,
			  uuid_t &uuid) {
    char *super_buf = read_object_hdr(name, false);
    auto super_h = (obj_hdr*)super_buf;

    if (super_h->magic != LSVD_MAGIC || super_h->version != 1 ||
	super_h->type != LSVD_SUPER)
	return std::make_pair((char*)NULL,-1);
    memcpy(uuid, super_h->vol_uuid, sizeof(uuid_t));

    super_hdr *super_sh = (super_hdr*)(super_h+1);

    decode_offset_len<uint32_t>(super_buf, super_sh->ckpts_offset,
				super_sh->ckpts_len, ckpts);
    decode_offset_len_ptr<clone_info>(super_buf, super_sh->clones_offset,
				      super_sh->clones_len, clones);
    decode_offset_len_ptr<snap_info>(super_buf, super_sh->snaps_offset,
				     super_sh->snaps_len, snaps);

    return std::make_pair(super_buf,super_sh->vol_size * 512);
}

/* read and decode the header of an object. Copies into arguments,
 * frees all allocated memory
 */
ssize_t object_reader::read_data_hdr(const char *name, obj_hdr &h,
				     obj_data_hdr &dh,
				     std::vector<obj_cleaned> &cleaned,
				     std::vector<data_map> &dmap) {
    char *buf = read_object_hdr(name, false);
    if (buf == NULL)
	return -1;
    auto tmp_h = (obj_hdr*)buf;
    auto tmp_dh = (obj_data_hdr*)(tmp_h+1);
    if (tmp_h->type != LSVD_DATA) {
	free(buf);
	return -1;
    }

    h = *tmp_h;
    dh = *tmp_dh;

    decode_offset_len<obj_cleaned>(buf, tmp_dh->objs_cleaned_offset,
				   tmp_dh->objs_cleaned_len, cleaned);
    decode_offset_len<data_map>(buf, tmp_dh->data_map_offset,
				tmp_dh->data_map_len, dmap);

    free(buf);
    return 0;
}

/* read and decode a checkpoint object identified by sequence number
 */
ssize_t object_reader::read_checkpoint(const char *name, uint64_t &cache_seq,
				       std::vector<uint32_t> &ckpts,
				       std::vector<ckpt_obj> &objects, 
				       std::vector<deferred_delete> &deletes,
				       std::vector<ckpt_mapentry> &dmap) {
    char *buf = read_object_hdr(name, false);
    if (buf == NULL) {
	do_log("buf == NULL\n");
	return -1;
    }
    auto h = (obj_hdr*)buf;
    auto ch = (obj_ckpt_hdr*)(h+1);
    if (h->type != LSVD_CKPT) {
	do_log("%s: WRONG TYPE %d\n", name, h->type);
	free(buf);
	return -1;
    }
    cache_seq = ch->cache_seq;
    decode_offset_len<uint32_t>(buf, ch->ckpts_offset, ch->ckpts_len, ckpts);
    decode_offset_len<ckpt_obj>(buf, ch->objs_offset, ch->objs_len, objects);
    decode_offset_len<deferred_delete>(buf, ch->deletes_offset,
				       ch->deletes_len, deletes);
    decode_offset_len<ckpt_mapentry>(buf, ch->map_offset,
				     ch->map_len, dmap);

    free(buf);
    return 0;
}

/* How many bytes will we need for an object header if we 
 * have @n_entries extent entries and @n_ckpts checkpoints. 
 */
size_t obj_hdr_len(int n_entries) {
    return sizeof(obj_hdr) +
	sizeof(obj_data_hdr) +
	n_entries * sizeof(data_map);
}

/* create header for a data object, returns size in bytes
 * unfortunately we need the length earlier in the code, so
 * we duplicate some of this logic in obj_hdr_len()
 */
size_t make_data_hdr(char *hdr, size_t bytes, uint64_t cache_seq,
		     std::vector<data_map> *entries, uint32_t seq,
		     uuid_t *uuid) {
    auto h = (obj_hdr*)hdr;
    auto dh = (obj_data_hdr*)(h+1);
    uint32_t o1 = sizeof(*h) + sizeof(*dh),
	o2 = o1, l2 = entries->size() * sizeof(data_map),
	hdr_bytes = o2 + l2;
    uint32_t hdr_sectors = div_round_up(hdr_bytes, 512);

    *h = (obj_hdr){.magic = LSVD_MAGIC, .version = 1, .vol_uuid = {0},
		   .type = LSVD_DATA, .seq = seq,
		   .hdr_sectors = hdr_sectors,
		   .data_sectors = (uint32_t)(bytes / 512), .crc = 0};
    memcpy(h->vol_uuid, uuid, sizeof(uuid_t));

    *dh = (obj_data_hdr){.cache_seq = cache_seq,
			 .objs_cleaned_offset = 0, . objs_cleaned_len = 0,
			 .data_map_offset = o2, .data_map_len = l2};

    auto dm = (data_map*)(dh+1);
    for (auto e : *entries)
	*dm++ = e;

    auto pad = hdr + hdr_bytes;	// make valgrind happy
    memset(pad, 0, hdr_sectors*512 - hdr_bytes);
    
    auto ptr = (const unsigned char *)hdr;
    h->crc = (uint32_t)crc32(0, ptr, hdr_sectors*512);

    return (char*)dm - (char*)hdr;
}
