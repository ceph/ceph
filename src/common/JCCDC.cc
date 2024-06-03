// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include "JCCDC.h"

// Unlike FastCDC described in the paper, if we are close to the
// target, use the target mask.  If we are very small or very large,
// use an adjusted mask--like the paper.  This tries to keep more
// cut points using the same mask, and fewer using the small or large
// masks.

// How many more/fewer bits to set in the small/large masks.
//
// This is the "normalization level" or "NC level" in the FastCDC
// paper.
#define TARGET_WINDOW_MASK_BITS 2

// How big the 'target window' is (in which we use the target mask).
//
// In the FastCDC paper, this is always 0: there is not "target
// window," and either small_mask (maskS) or large_mask (maskL) is
// used--never target_mask (maskA).
#define TARGET_WINDOW_BITS 1

// How many bits larger/smaller than target for hard limits on chunk
// size.
//
// We assume the min and max sizes are always this many bits
// larger/smaller than the target.  (Note that the FastCDC paper 8KB
// example has a min of 2KB (2 bits smaller) and max of 64 KB (3 bits
// larger), although it is not clear why they chose those values.)
#define SIZE_WINDOW_BITS 2

void JCCDC::_setup(int target, int size_window_bits){
    target_bits = target;

    if (!size_window_bits) {
        size_window_bits = SIZE_WINDOW_BITS;
    }
    min_bits = target - size_window_bits;
    max_bits = target + size_window_bits;

    std::mt19937_64 engine;

    //init prehash table
    #define SeedLength 64
    char seed[SeedLength];
    for (int i = 0; i < SymbolCount; i++) {
        for (int j = 0; j < SeedLength; j++) {
            seed[j] = i;
        }

        prehash[i] = 0;
        unsigned char md5_result[DigistLength];

        MD5_CTX md5_ctx;
        MD5_Init(&md5_ctx);
        MD5_Update(&md5_ctx, seed, SeedLength);
        MD5_Final(md5_result, &md5_ctx);

        memcpy(&prehash[i], md5_result, sizeof(uint64_t));
    }

    // set the jump mask, chunk mask and jump length
    ceph_assert(target_bits > 2);
    chunk_mask = JC_mask[target_bits-1];
    jump_mask = JC_mask[target_bits-2];
    jump_len = (1ul << target_bits)/2;

    printf("\nMask:  %16lx\n", Mask);
    printf("jumpMask:%16lx\n", jumpMask);
    printf("jumpLen:%d\n\n", jumpLen);

    // set mask
}


void JCCDC::calc_chunks(
    const bufferlist &bl,
    std::vector<std::pair<uint64_t, uint64_t>> *chunks) const{
    if (bl.length() == 0){
        return;
    }
    auto p = bl.buffers().begin();
    const char *pp = p->c_str();
    const char *pe = pp + p->length();

    size_t pos = 0;
    size_t len = bl.length();
    while (pos < len){
        size_t cstart = pos;
        uint64_t fp = 0;

        // are we left with a min-sized (or smaller) chunk?
        if (len - pos <= (1ul << min_bits)) {
            chunks->push_back(std::pair<uint64_t, uint64_t>(pos, len - pos));
            break;
        }

        // skip forward to the min chunk size cut point (minus the window, so
        // we can initialize the rolling fingerprint).
        size_t skip = (1 << min_bits) - window;
        pos += skip;
        // seems pp only contains a part of string
        while (skip){
            size_t s = std::min<size_t>(pe - pp, skip);
            skip -= s;
            pp += s;
            if (pp == pe)
            {
                ++p;
                pp = p->c_str();
                pe = pp + p->length();
            }
        }

        // first fill the window
        size_t max = pos + window;
        while (pos < max) {
            if (pp == pe) {
                ++p;
                pp = p->c_str();
                pe = pp + p->length();
            }
            const char *te = std::min(pe, pp + (max - pos));
            for (; pp < te; ++pp, ++pos) {
                fp = (fp << 1) + (prehash[*(unsigned char *)p]);
            }
        }
        ceph_assert(pos < len);

        // find an end marker
        // the tail of max window
        size_t tail = std::min(len,
             cstart + (1 << (target_bits + TARGET_WINDOW_BITS)));
        bool cut_condition = 0;
        while ((pos < tail) && (!cut_condition) ) {
            if (pp == pe){
                ++p;
                pp = p->c_str();
                pe = pp + p->length();
            }
            const char *te = std::min(pe, pp + tail - pos);
            for (; pp < te; ) {
                fp = (fp << 1) + (prehash[*(unsigned char *)pp]);
                ++pp; ++pos;

                if (G_UNLIKELY(!(fp & jump_mask))) {
                    if ((!(fp & chunk_mask))) { 
                        cut_condition = 1;
                    } else {
                        size_t skip = std:min(jump_len, tail-pos);
                        pos += skip;
                        // seems pp only contains a part of string
                        while (skip){
                            size_t s = std::min<size_t>(pe - pp, skip);
                            skip -= s;
                            pp += s;
                            if (pp == pe) {
                                ++p;
                                pp = p->c_str();
                                pe = pp + p->length();
                            }
                        }
                    }
                }
            }
        }
        chunks->push_back(std::pair<uint64_t, uint64_t>(cstart, pos - cstart));
    }
}
