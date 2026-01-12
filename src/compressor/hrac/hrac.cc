#include <cstdio>
#include <cstring>
#include <immintrin.h>
#include "hrac.h"

static const uint32_t nonzero_count_u8[256] = {
0, 
1, 
2, 2, 
3, 3, 3, 3, 
4, 4, 4, 4, 4, 4, 4, 4, 
5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 
6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8};

static inline uint32_t nonzero_count_u32(uint32_t x) noexcept {
    return 32 - _lzcnt_u32(x);
}

static const uint64_t bit_mask_u64[65] = {0, 0x1, 0x3, 0x7, 0xf, 0x1f, 0x3f, 0x7f, 0xff, 0x1ff, 0x3ff, 0x7ff, 0xfff, 0x1fff, 0x3fff, 0x7fff, 0xffff, 0x1ffff, 0x3ffff, 0x7ffff, 0xfffff, 0x1fffff, 0x3fffff, 0x7fffff, 0xffffff, 0x1ffffff, 0x3ffffff, 0x7ffffff, 0xfffffff, 0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffff, 0x1ffffffff, 0x3ffffffff, 0x7ffffffff, 0xfffffffff, 0x1fffffffff, 0x3fffffffff, 0x7fffffffff, 0xffffffffff, 0x1ffffffffff, 0x3ffffffffff, 0x7ffffffffff, 0xfffffffffff, 0x1fffffffffff, 0x3fffffffffff, 0x7fffffffffff, 0xffffffffffff, 0x1ffffffffffff, 0x3ffffffffffff, 0x7ffffffffffff, 0xfffffffffffff, 0x1fffffffffffff, 0x3fffffffffffff, 0x7fffffffffffff, 0xffffffffffffff, 0x1ffffffffffffff, 0x3ffffffffffffff, 0x7ffffffffffffff, 0xfffffffffffffff, 0x1fffffffffffffff, 0x3fffffffffffffff, 0x7fffffffffffffff, 0xffffffffffffffff};


static inline void output_nbits_m64(uint64_t*& ocur, uint32_t& obleft, uint64_t v, const uint32_t n) {
    if (n==64) {
        *ocur |= v << obleft;
        ocur += 1;
        if(obleft)
            *ocur = v >> (64 - obleft);
        return;
    }
    v &= (1ul<<n)-1;
    *ocur |= v << obleft;
    obleft += n;

    if (obleft >= 64) {
        ocur += 1;
        *ocur = 0;
        obleft -= 64;
        *ocur |= v >> (n - obleft);
    }
}

static inline void output_nbits(uint64_t*& ocur, uint32_t& obleft, uint64_t v, const uint32_t n) {
    v &= (1ul<<n)-1;
    *ocur |= v << obleft;
    obleft += n;

    if (obleft >= 64) {
        ocur += 1;
        *ocur = 0;
        obleft -= 64;
        *ocur |= v >> (n - obleft);
    }
}

static inline void done_outputing_bits(uint64_t*& ocur, uint32_t& obleft) {
    uint8_t *t = (uint8_t*)ocur;
    t += (obleft + 7) / 8;
    ocur = (uint64_t*)t;
}

static inline uint64_t input_nbits(uint64_t*& icur, uint32_t& ibleft, const uint32_t& n) {
    uint64_t v = (*icur >> (ibleft));
    ibleft += n;
    v &= (1ul<<n)-1;
    if (ibleft >= 64) {
        ibleft -= 64;
        icur++;
        v |= (*icur & ((1ul<<ibleft)-1)) << (n - ibleft);
    }
    return v;
}

static uint32_t input_cnt0p1(uint64_t*& icur, uint32_t& ibleft) {
    uint64_t l = *icur >> ibleft;
    uint32_t cnt;
    if (l == 0) {
        cnt = 64 - ibleft;
        icur++;
        uint32_t t = _tzcnt_u64(*icur);
        ibleft = t + 1;
        return cnt + t;
    }
    else {
        cnt = _tzcnt_u64(l);
        ibleft += cnt + 1;
        if (ibleft >= 64) {
            ibleft -= 64;
            icur++;
        }
        return cnt;
    }
}






// -------------------------
static inline uint8_t diff_e8(uint8_t avg, uint8_t v) {
    int8_t  t  = (int8_t)(v - avg);
    uint8_t s  = (uint8_t)(t >> 7);
    uint8_t t2 = (uint8_t)(t << 1);
    return t2 ^ s;
}
static inline uint8_t diff_d8(uint8_t avg, uint8_t d) {
    uint8_t abs  = d >> 1;
    uint8_t mask = - (d & 1u);
    return avg + (abs ^ mask);
}

static inline uint16_t diff_e16(uint16_t avg, uint16_t v) {
    int16_t  t  = (int16_t)(v - avg);
    uint16_t s  = (uint16_t)(t >> 15);
    uint16_t t2 = (uint16_t)(t << 1);
    return t2 ^ s;
}
static inline uint16_t diff_d16(uint16_t avg, uint16_t d) {
    uint16_t abs  = d >> 1;
    uint16_t mask = - (d & 1u);
    return avg + (abs ^ mask);
}

static inline uint32_t diff_e32(uint32_t avg, uint32_t v) {
    int32_t  t  = (int32_t)(v - avg);
    uint32_t s  = (uint32_t)(t >> 31);
    uint32_t t2 = (uint32_t)(t << 1);
    return t2 ^ s;
}
static inline uint32_t diff_d32(uint32_t avg, uint32_t d) {
    uint32_t abs  = d >> 1;
    uint32_t mask = - (d & 1u);
    return avg + (abs ^ mask);
}

// -------------------------------------

#define dtype uint32_t
#define diff_e diff_e32
#define diff_d diff_d32
#define BW 32
#define LBW 5
#define sig_len(x) nonzero_count_u32(x)

uint32_t fits_kdecomp_f64(
    const uint8_t  in[],
    const uint32_t iLen,
    double  out_[],
    uint32_t oLen,
    const uint32_t blk,
    const uint32_t nsblk,
    const uint32_t inner
) {
    uint32_t bigBlock = nsblk * inner;
    uint64_t* icur = (uint64_t*)in;
    uint32_t ibleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;
    uint32_t* out = (uint32_t*)out_;

    dtype* rw = (dtype*)aligned_alloc(64, sizeof(dtype)*lft);
    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    if (!bkt || !rw) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }

    uint32_t k=0;
    for (; (uint8_t*)icur - in + (ibleft + 7) / 8 < iLen - 1; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) 
        {
            uint32_t i=j;
            uint32_t avg_l, bas_l, sign_l;

            for (uint32_t x=0; x < lft; x++)
                out[(i + x*inner)*2] = input_nbits(icur, ibleft, BW);

            sign_l = input_nbits(icur, ibleft, 1);
            
            bas_l = input_nbits(icur, ibleft, LBW);
            
            if (bas_l == BW-1 && input_nbits(icur, ibleft, 1) == 0)
                bas_l = BW;          


            if (bas_l != BW-1) {
                avg_l = input_nbits(icur, ibleft, BW);
            }
                
            

            if (bas_l == BW) {
                for (uint32_t x=0; x < lft; x++)
                    out[(i + x*inner)*2+1] = avg_l;
            }
            else if (bas_l < BW-1) {
                for (uint32_t x=0; x < lft; x++) {
                    uint32_t od = input_cnt0p1(icur, ibleft), df;
                    if (!od) {
                        df = input_nbits(icur, ibleft, bas_l);
                    }
                    else {
                        od += bas_l - 1u;
                        df = input_nbits(icur, ibleft, od) + (1u << od);
                    }
                    out[(i + x*inner)*2+1] = diff_d(avg_l, df);
                }
            }
            else {
                for (uint32_t x=0; x < lft; x++) {
                    out[(i + x*inner)*2+1] = input_nbits(icur, ibleft, BW);
                }
            }
            if (sign_l) {
                for (uint32_t x=0; x < lft; x++) {
                    out[(i + x*inner)*2+1] = ((0x00100000&out[(i + x*inner)*2+1])<<11) + ((0xffe00000&out[(i + x*inner)*2+1])>>1) + (0x000fffff&out[(i + x*inner)*2+1]);
                }
            }
            i += inner*lft;
            

            
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c, st;
                uint64_t avg_c=0, sign_c=0;
                dtype mx=0, mn=(1ul<<BW)-1u;

                for (uint32_t x=0; x < blk; x++)
                    out[(i + x*inner)*2] = input_nbits(icur, ibleft, BW);

                if (bas_l == BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        rw[x] = input_nbits(icur, ibleft, BW);
                    }

                }
                else if (bas_l < BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        uint32_t od = input_cnt0p1(icur, ibleft), df;
                        if (!od) {
                            df = input_nbits(icur, ibleft, bas_l);
                        }
                        else {
                            od += bas_l - 1u;
                            df = input_nbits(icur, ibleft, od) + (1ul << od);
                        }
                        rw[x] = diff_d(avg_l, df);
                    }

                }
                else {
                    if (input_nbits(icur, ibleft, 1)) {
                        
                        if (sign_l) {
                            for (uint32_t x=0; x<blk; x++) {
                                out[(i + x*inner)*2+1] = ((0x00100000&avg_l)<<11) + ((0xffe00000&avg_l)>>1) + (0x000fffff&avg_l);
                            }
                        }
                        else
                            for (uint32_t x=0; x<blk; x++) {
                                out[(i + x*inner)*2+1] = avg_l;
                            }
                        i += inner*blk;
                        continue;
                    }
                    else {
                        bas_l = 0;
                        for (uint32_t x=0; x < blk; x++) {
                            uint32_t od = input_cnt0p1(icur, ibleft), df;
                            if (!od) {
                                df = input_nbits(icur, ibleft, bas_l);
                            }
                            else {
                                od += bas_l - 1u;
                                df = input_nbits(icur, ibleft, od) + (1ul << od);
                            }
                            rw[x] = diff_d(avg_l, df);
                        }

                    }
                }



                for (uint32_t x = 0; x < blk; x++) {
                    mx = mx < rw[x] ? rw[x] : mx;
                    mn = mn > rw[x] ? rw[x] : mn;
                    avg_c += rw[x];
                }
                avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                if (mx == mn) {
                    bas_c = BW;
                    if (sign_l)
                        for (uint32_t x=0; x < blk; x++) {
                            out[(i + x*inner)*2+1] = ((0x00100000&rw[x])<<11) + ((0xffe00000&rw[x])>>1) + (0x000fffff&rw[x]);
                            sign_c += ((0x00100000&rw[x])<<11);
                        }
                    else
                        for (uint32_t x=0; x < blk; x++) {
                            out[(i + x*inner)*2+1] = rw[x];
                            sign_c += rw[x] & (1u << 31);
                        }
                }
                else {
                    memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                    if (sign_l)
                        for (uint32_t x = 0; x < blk; x++) {
                            out[(i + x*inner)*2+1] = ((0x00100000&rw[x])<<11) + ((0xffe00000&rw[x])>>1) + (0x000fffff&rw[x]);
                            sign_c += ((0x00100000&rw[x])<<11);
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                    else
                        for (uint32_t x = 0; x < blk; x++) {
                            out[(i + x*inner)*2+1] = rw[x];
                            sign_c += rw[x] & (1u << 31);
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                    for (uint32_t x = 0, sum = 0; x < BW; x++) {
                        sum += bkt[x];
                        if (sum >= blk - sum - bkt[x+1]) {
                            bas_c = x;
                            break;
                        }
                    }
                }

                sign_c >>= 31;
                if (sign_c > blk/2)
                    sign_c = blk - sign_c;
                if (sign_c > (blk>>4)) 
                    sign_c = 1;
                else
                    sign_c = 0;

                bas_l = bas_c;
                avg_l = avg_c;
                sign_l = sign_c;
                i += inner*blk;
            }

        }
        //break;
    }


    free(bkt);
    free(rw);
    return k;
}

uint32_t fits_kcomp_f64(
    const double in_[],		    /* input array */
    const uint32_t iLen,		/* input length */
    uint8_t out[],	            /* output buffer */
    uint32_t oLen,		        /* max length of output */
    const uint32_t blk,         /* parameter that controls the block size */
    const uint32_t nsblk,       /* length of dimension with lowest variability */
    const uint32_t inner        /* the stride along this dimension */
) {
    uint32_t bigBlock = nsblk * inner;
    if (!bigBlock || iLen%bigBlock) {
        printf("error: dimensions don't match.\n");
        return 0;
    }
    if (blk < 16) {
        printf("error: blk should >= 16.\n");
        return 0;
    }
    if (nsblk < blk) {
        printf("error: nsblk should >= blk.\n");
        return 0;
    }
    if (iLen > (1u<<31)) {
        printf("warning: the input is too large.\n");
    }
    if (iLen + iLen/nsblk*(BW+LBW) > oLen) { 
        printf("warning: possible buffer overflow.\n");
    }// Under maliciously crafted input, the compress bound may reach iLen*2 + iLen/nsblk*(BW+LBW) + iLen/(16*blk)


    uint64_t* ocur = (uint64_t*)out;
    uint32_t obleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;
    uint32_t* in = (uint32_t*)in_;
    *ocur = 0;

    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    dtype*    rw  = (dtype*)   aligned_alloc(64, sizeof(dtype)*lft);
    uint64_t* ci  = (uint64_t*)aligned_alloc(64, sizeof(uint64_t)*lft);
    uint32_t* li  = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*lft);

    if (!bkt || !rw || !ci || !li) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }


    for (uint32_t k=0; k<iLen; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) {
            uint32_t i=j, bas_l;
            uint64_t avg_l=0, sign_l=0;
            dtype mx=0, mn=(1ul<<BW)-1u;


            for (uint32_t x = 0; x < lft; x++) {
                output_nbits(ocur, obleft, in[(i + inner*x)*2], BW);
                rw[x] = in[(i + inner*x)*2+1];
                sign_l += rw[x] & (1u << 31);
            }
            i += inner*lft;

            sign_l >>= 31;
            if (sign_l > blk/2)
                sign_l = blk - sign_l;
            if (sign_l > (blk>>4)) {
                output_nbits(ocur, obleft, 1, 1);
                for (uint32_t x = 0; x < lft; x++) {
                    rw[x] = ((0x7ff00000&rw[x])<<1) + ((0x80000000&rw[x])>>11) + ((0x000fffff&rw[x])); 
                }
                sign_l = 1;
            }
            else {
                output_nbits(ocur, obleft, 0, 1);
                sign_l = 0;
            }

            for (uint32_t x = 0; x < lft; x++) {
                mx = mx < rw[x] ? rw[x] : mx;
                mn = mn > rw[x] ? rw[x] : mn;
                avg_l += rw[x];
            }
            avg_l = (avg_l + lft - 3 - mn - mx) / (lft - 2);

            
            


            if (mx == mn) {
                bas_l = BW;
            }
            else {
                memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                for (uint32_t x = 0; x < lft; x++) {
                    bkt[sig_len(diff_e(avg_l, rw[x]))]++;
                }
                for (uint32_t x = 0, sum = 0; x < BW; x++) {
                    sum += bkt[x];
                    if (sum > lft - sum - bkt[x+1]) {
                        bas_l = x;
                        break;
                    }
                }
            }


            if (bas_l < BW-1)
                output_nbits(ocur, obleft, bas_l, LBW);
            else if (bas_l == BW-1)
                output_nbits(ocur, obleft, (BW-1)+BW, LBW+1);
            else
                output_nbits(ocur, obleft, BW-1, LBW+1);
            


            if (bas_l != BW-1)
                output_nbits(ocur, obleft, avg_l, BW);

            if (bas_l < BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    ci[x] = diff_e(avg_l, rw[x]);
                    uint32_t m = sig_len(ci[x]>>bas_l);
                    ci[x] = ((ci[x]<<1u)|1u)<<m;
                    li[x] = bas_l + !m + m + m;
                    output_nbits_m64(ocur, obleft, ci[x], li[x]);
                }
            else if (bas_l == BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    output_nbits(ocur, obleft, rw[x], BW);
                }

            // ---
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c;
                uint64_t avg_c=0, sign_c=0;
                mx=0, mn=(1ul<<BW)-1u;

                if (bas_l == BW-1) {  // random
                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, in[(i + inner*x)*2], BW);
                        rw[x] = in[(i + inner*x)*2+1];
                        sign_c += rw[x] & (1u << 31);
                    }
                    i += inner*blk;

                    if (sign_l) {
                        for (uint32_t x = 0; x < blk; x++) {
                            rw[x] = ((0x7ff00000&rw[x])<<1) + ((0x80000000&rw[x])>>11) + ((0x000fffff&rw[x]));
                        }
                    }
                    sign_c >>= 31;
                    if (sign_c > blk/2)
                        sign_c = blk - sign_c;
                    if (sign_c > (blk>>4)) 
                        sign_c = 1;
                    else
                        sign_c = 0;


                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, rw[x], BW);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                    }
                }
                else if (bas_l == BW) {  // same
                    bas_l = 0;
                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, in[(i + inner*x)*2], BW);
                        rw[x] = in[(i + inner*x)*2+1];
                        sign_c += rw[x] & (1u << 31);
                    }
                    i += inner*blk;
                    if (sign_l) {
                        for (uint32_t x = 0; x < blk; x++) {
                            rw[x] = ((0x7ff00000&rw[x])<<1) + ((0x80000000&rw[x])>>11) + ((0x000fffff&rw[x])); 
                        }
                    }
                    sign_c >>= 31;
                    if (sign_c > blk/2)
                        sign_c = blk - sign_c;
                    if (sign_c > (blk>>4)) 
                        sign_c = 1;
                    else
                        sign_c = 0;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);

                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    if (mx == mn && mx == avg_l) {
                        output_nbits(ocur, obleft, 1, 1);
                        avg_c = avg_l;
                        bas_c = BW;
                    }
                    else {
                        output_nbits(ocur, obleft, 0, 1);
                        avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                        if (mx == mn)
                            bas_c = BW;
                    }
                }
                else { // common
                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, in[(i + inner*x)*2], BW);
                        rw[x] = in[(i + inner*x)*2+1];
                        sign_c += rw[x] & (1u << 31);
                    }
                    i += inner*blk;
                    if (sign_l) {
                        for (uint32_t x = 0; x < blk; x++) {
                            rw[x] = ((0x7ff00000&rw[x])<<1) + ((0x80000000&rw[x])>>11) + ((0x000fffff&rw[x])); 
                        }
                    }
                    sign_c >>= 31;
                    if (sign_c > blk/2)
                        sign_c = blk - sign_c;
                    if (sign_c > (blk>>4)) 
                        sign_c = 1;
                    else
                        sign_c = 0;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);
                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        for (uint32_t x = 0; x < blk; x++) {
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }

                    }

                }
                bas_l = bas_c;
                avg_l = avg_c;
                sign_l = sign_c;
                
            }
        }

    }

    done_outputing_bits(ocur, obleft);
    free(bkt);
    free(rw);
    free(ci);
    free(li);
    return (uint8_t*)ocur - out;
}

uint32_t fits_kdecomp_f32(
    const uint8_t  in[],
    const uint32_t iLen,
    float  out_[],
    uint32_t oLen,
    const uint32_t blk,
    const uint32_t nsblk,
    const uint32_t inner
) {
    uint32_t bigBlock = nsblk * inner;
    uint64_t* icur = (uint64_t*)in;
    uint32_t ibleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;
    uint32_t* out = (uint32_t*)out_;

    dtype* rw = (dtype*)aligned_alloc(64, sizeof(dtype)*lft);
    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    if (!bkt || !rw) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }

    uint32_t k=0;
    for (; (uint8_t*)icur - in + (ibleft + 7) / 8 < iLen - 1; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) 
        {
            uint32_t i=j;
            uint32_t avg_l, bas_l, sign_l;

            sign_l = input_nbits(icur, ibleft, 1);
            
            bas_l = input_nbits(icur, ibleft, LBW);
            
            if (bas_l == BW-1 && input_nbits(icur, ibleft, 1) == 0)
                bas_l = BW;          


            if (bas_l != BW-1) {
                avg_l = input_nbits(icur, ibleft, BW);
            }
                
            

            if (bas_l == BW) {
                for (uint32_t x=0; x < lft; x++)
                    out[i + x*inner] = avg_l;
            }
            else if (bas_l < BW-1) {
                for (uint32_t x=0; x < lft; x++) {
                    uint32_t od = input_cnt0p1(icur, ibleft), df;
                    if (!od) {
                        df = input_nbits(icur, ibleft, bas_l);
                    }
                    else {
                        od += bas_l - 1u;
                        df = input_nbits(icur, ibleft, od) + (1u << od);
                    }
                    out[i + x*inner] = diff_d(avg_l, df);
                }
            }
            else {
                for (uint32_t x=0; x < lft; x++) {
                    out[i + x*inner] = input_nbits(icur, ibleft, BW);
                }
            }
            if (sign_l) {
                for (uint32_t x=0; x < lft; x++) {
                    out[i + x*inner] = ((0x00800000&out[i + x*inner])<<8) + ((0xff000000&out[i + x*inner])>>1) + (0x007fffff&out[i + x*inner]);
                }
            }
            i += inner*lft;
            

            
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c, st;
                uint64_t avg_c=0, sign_c=0;
                dtype mx=0, mn=(1ul<<BW)-1u;

                if (bas_l == BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        rw[x] = input_nbits(icur, ibleft, BW);
                    }

                }
                else if (bas_l < BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        uint32_t od = input_cnt0p1(icur, ibleft), df;
                        if (!od) {
                            df = input_nbits(icur, ibleft, bas_l);
                        }
                        else {
                            od += bas_l - 1u;
                            df = input_nbits(icur, ibleft, od) + (1ul << od);
                        }
                        rw[x] = diff_d(avg_l, df);
                    }

                }
                else {
                    if (input_nbits(icur, ibleft, 1)) {
                        if (sign_l) {
                            for (uint32_t x=0; x<blk; x++) {
                                out[i + x*inner] = ((0x00800000&avg_l)<<8) + ((0xff000000&avg_l)>>1) + (0x007fffff&avg_l);
                            }
                        }
                        else
                            for (uint32_t x=0; x<blk; x++) {
                                out[i + x*inner] = avg_l;
                            }
                        i += inner*blk;
                        continue;
                    }
                    else {
                        bas_l = 0;
                        for (uint32_t x=0; x < blk; x++) {
                            uint32_t od = input_cnt0p1(icur, ibleft), df;
                            if (!od) {
                                df = input_nbits(icur, ibleft, bas_l);
                            }
                            else {
                                od += bas_l - 1u;
                                df = input_nbits(icur, ibleft, od) + (1ul << od);
                            }
                            rw[x] = diff_d(avg_l, df);
                        }

                    }
                }



                for (uint32_t x = 0; x < blk; x++) {
                    mx = mx < rw[x] ? rw[x] : mx;
                    mn = mn > rw[x] ? rw[x] : mn;
                    avg_c += rw[x];
                }
                avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                if (mx == mn) {
                    bas_c = BW;
                    if (sign_l)
                        for (uint32_t x=0; x < blk; x++) {
                            out[i + x*inner] = ((0x00800000&rw[x])<<8) + ((0xff000000&rw[x])>>1) + (0x007fffff&rw[x]);
                            sign_c += ((0x00800000&rw[x])<<8);
                        }
                    else
                        for (uint32_t x=0; x < blk; x++) {
                            out[i + x*inner] = rw[x];
                            sign_c += rw[x] & (1u << 31);
                        }
                }
                else {
                    memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                    if (sign_l)
                        for (uint32_t x = 0; x < blk; x++) {
                            out[i + x*inner] = ((0x00800000&rw[x])<<8) + ((0xff000000&rw[x])>>1) + (0x007fffff&rw[x]);
                            sign_c += ((0x00800000&rw[x])<<8);
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                    else
                        for (uint32_t x = 0; x < blk; x++) {
                            out[i + x*inner] = rw[x];
                            sign_c += rw[x] & (1u << 31);
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                    for (uint32_t x = 0, sum = 0; x < BW; x++) {
                        sum += bkt[x];
                        if (sum >= blk - sum - bkt[x+1]) {
                            bas_c = x;
                            break;
                        }
                    }
                }

                sign_c >>= 31;
                if (sign_c > blk/2)
                    sign_c = blk - sign_c;
                if (sign_c > (blk>>4)) 
                    sign_c = 1;
                else
                    sign_c = 0;

                bas_l = bas_c;
                avg_l = avg_c;
                sign_l = sign_c;
                i += inner*blk;
            }

        }
        //break;
    }


    free(bkt);
    free(rw);
    return k;
}

uint32_t fits_kcomp_f32(
    const float in_[],		    /* input array */
    const uint32_t iLen,		/* input length */
    uint8_t out[],	            /* output buffer */
    uint32_t oLen,		        /* max length of output */
    const uint32_t blk,         /* parameter that controls the block size */
    const uint32_t nsblk,       /* length of dimension with lowest variability*/
    const uint32_t inner        /* the stride along this dimension */
) {
    uint32_t bigBlock = nsblk * inner;
    if (!bigBlock || iLen%bigBlock) {
        printf("error: dimensions don't match.\n");
        return 0;
    }
    if (blk < 16) {
        printf("error: blk should >= 16.\n");
        return 0;
    }
    if (nsblk < blk) {
        printf("error: nsblk should >= blk.\n");
        return 0;
    }
    if (iLen > (1u<<31)) {
        printf("warning: the input is too large.\n");
    }
    if (iLen + iLen/nsblk*(BW+LBW) > oLen) { 
        printf("warning: possible buffer overflow.\n");
    }// Under maliciously crafted input, the compress bound may reach iLen*2 + iLen/nsblk*(BW+LBW) + iLen/(16*blk)


    uint64_t* ocur = (uint64_t*)out;
    uint32_t obleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;
    uint32_t* in = (uint32_t*)in_;
    *ocur = 0;

    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    dtype*    rw  = (dtype*)   aligned_alloc(64, sizeof(dtype)*lft);
    uint64_t* ci  = (uint64_t*)aligned_alloc(64, sizeof(uint64_t)*lft);
    uint32_t* li  = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*lft);

    if (!bkt || !rw || !ci || !li) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }


    for (uint32_t k=0; k<iLen; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) {
            uint32_t i=j, bas_l;
            uint64_t avg_l=0, sign_l=0;
            dtype mx=0, mn=(1ul<<BW)-1u;


            for (uint32_t x = 0; x < lft; x++) {
                rw[x] = in[i + inner*x];
                sign_l += rw[x] & (1u << 31);
            }
            i += inner*lft;

            sign_l >>= 31;
            if (sign_l > blk/2)
                sign_l = blk - sign_l;
            if (sign_l > (blk>>4)) {
                output_nbits(ocur, obleft, 1, 1);
                for (uint32_t x = 0; x < lft; x++) {
                    rw[x] = ((0x7f800000&rw[x])<<1) + ((0x80000000&rw[x])>>8) + ((0x007fffff&rw[x])); 
                }
                sign_l = 1;
            }
            else {
                output_nbits(ocur, obleft, 0, 1);
                sign_l = 0;
            }

            for (uint32_t x = 0; x < lft; x++) {
                mx = mx < rw[x] ? rw[x] : mx;
                mn = mn > rw[x] ? rw[x] : mn;
                avg_l += rw[x];
            }
            avg_l = (avg_l + lft - 3 - mn - mx) / (lft - 2);

            
            


            if (mx == mn) {
                bas_l = BW;
            }
            else {
                memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                for (uint32_t x = 0; x < lft; x++) {
                    bkt[sig_len(diff_e(avg_l, rw[x]))]++;
                }
                for (uint32_t x = 0, sum = 0; x < BW; x++) {
                    sum += bkt[x];
                    if (sum >= lft - sum - bkt[x+1]) {
                        bas_l = x;
                        break;
                    }
                }
            }


            if (bas_l < BW-1)
                output_nbits(ocur, obleft, bas_l, LBW);
            else if (bas_l == BW-1)
                output_nbits(ocur, obleft, (BW-1)+BW, LBW+1);
            else
                output_nbits(ocur, obleft, BW-1, LBW+1);
            


            if (bas_l != BW-1)
                output_nbits(ocur, obleft, avg_l, BW);

            if (bas_l < BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    ci[x] = diff_e(avg_l, rw[x]);
                    uint32_t m = sig_len(ci[x]>>bas_l);
                    ci[x] = ((ci[x]<<1u)|1u)<<m;
                    li[x] = bas_l + !m + m + m;
                    output_nbits_m64(ocur, obleft, ci[x], li[x]);
                }
            else if (bas_l == BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    output_nbits(ocur, obleft, rw[x], BW);
                }

            // ---
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c;
                uint64_t avg_c=0, sign_c=0;
                mx=0, mn=(1ul<<BW)-1u;

                if (bas_l == BW-1) {  // random
                    for (uint32_t x = 0; x < blk; x++) {
                        rw[x] = in[i + inner*x];
                        sign_c += rw[x] & (1u << 31);
                    }
                    i += inner*blk;

                    if (sign_l) {
                        for (uint32_t x = 0; x < blk; x++) {
                            rw[x] = ((0x7f800000&rw[x])<<1) + ((0x80000000&rw[x])>>8) + ((0x007fffff&rw[x])); 
                        }
                    }
                    sign_c >>= 31;
                    if (sign_c > blk/2)
                        sign_c = blk - sign_c;
                    if (sign_c > (blk>>4)) 
                        sign_c = 1;
                    else
                        sign_c = 0;


                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, rw[x], BW);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                    }
                }
                else if (bas_l == BW) {  // same
                    bas_l = 0;
                    for (uint32_t x = 0; x < blk; x++) {
                        rw[x] = in[i + inner*x];
                        sign_c += rw[x] & (1u << 31);
                    }
                    i += inner*blk;
                    if (sign_l) {
                        for (uint32_t x = 0; x < blk; x++) {
                            rw[x] = ((0x7f800000&rw[x])<<1) + ((0x80000000&rw[x])>>8) + ((0x007fffff&rw[x])); 
                        }
                    }
                    sign_c >>= 31;
                    if (sign_c > blk/2)
                        sign_c = blk - sign_c;
                    if (sign_c > (blk>>4)) 
                        sign_c = 1;
                    else
                        sign_c = 0;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);

                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    if (mx == mn && mx == avg_l) {
                        output_nbits(ocur, obleft, 1, 1);
                        avg_c = avg_l;
                        bas_c = BW;
                    }
                    else {
                        output_nbits(ocur, obleft, 0, 1);
                        avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                        if (mx == mn)
                            bas_c = BW;
                    }
                }
                else { // common
                    for (uint32_t x = 0; x < blk; x++) {
                        rw[x] = in[i + inner*x];
                        sign_c += rw[x] & (1u << 31);
                    }
                    i += inner*blk;
                    if (sign_l) {
                        for (uint32_t x = 0; x < blk; x++) {
                            rw[x] = ((0x7f800000&rw[x])<<1) + ((0x80000000&rw[x])>>8) + ((0x007fffff&rw[x])); 
                        }
                    }
                    sign_c >>= 31;
                    if (sign_c > blk/2)
                        sign_c = blk - sign_c;
                    if (sign_c > (blk>>4)) 
                        sign_c = 1;
                    else
                        sign_c = 0;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);
                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        for (uint32_t x = 0; x < blk; x++) {
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }

                    }

                }
                bas_l = bas_c;
                avg_l = avg_c;
                sign_l = sign_c;
                
            }
        }

    }

    done_outputing_bits(ocur, obleft);
    free(bkt);
    free(rw);
    free(ci);
    free(li);
    return (uint8_t*)ocur - out;
}


uint32_t fits_kdecomp_u32(
    const uint8_t  in[],
    const uint32_t iLen,
    dtype  out[],
    uint32_t oLen,
    const uint32_t blk,
    const uint32_t nsblk,
    const uint32_t inner
) {
    uint32_t bigBlock = nsblk * inner;
    uint64_t* icur = (uint64_t*)in;
    uint32_t ibleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;

    dtype* rw = (dtype*)aligned_alloc(64, sizeof(dtype)*lft);
    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    if (!bkt || !rw) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }

    uint32_t k=0;
    for (; (uint8_t*)icur - in + (ibleft + 7) / 8 < iLen - 1; k+=bigBlock) {
        
        for (uint32_t j=k; j<k+inner; j++) 
        {
            uint32_t i=j;
            uint32_t avg_l, bas_l;
            
            bas_l = input_nbits(icur, ibleft, LBW);
            
            if (bas_l == BW-1 && input_nbits(icur, ibleft, 1) == 0)
                bas_l = BW;          


            if (bas_l != BW-1) {
                avg_l = input_nbits(icur, ibleft, BW);
            }
            
            

            if (bas_l == BW) {
                for (uint32_t x=0; x < lft; x++)
                    out[i + x*inner] = avg_l;
            }
            else if (bas_l < BW-1) {
                for (uint32_t x=0; x < lft; x++) {
                    uint32_t od = input_cnt0p1(icur, ibleft), df;
                    if (!od) {
                        df = input_nbits(icur, ibleft, bas_l);
                    }
                    else {
                        od += bas_l - 1u;
                        df = input_nbits(icur, ibleft, od) + (1u << od);
                    }
                    out[i + x*inner] = diff_d(avg_l, df);
                }
            }
            else {
                for (uint32_t x=0; x < lft; x++) {
                    out[i + x*inner] = input_nbits(icur, ibleft, BW);
                }
            }
            i += inner*lft;
            

            
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c;
                uint64_t avg_c=0;
                dtype mx=0, mn=(1ul<<BW)-1u;

                if (bas_l == BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        rw[x] = input_nbits(icur, ibleft, BW);
                        out[i + x*inner] = rw[x];
                    }
                }
                else if (bas_l < BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        uint32_t od = input_cnt0p1(icur, ibleft), df;
                        if (!od) {
                            df = input_nbits(icur, ibleft, bas_l);
                        }
                        else {
                            od += bas_l - 1u;
                            df = input_nbits(icur, ibleft, od) + (1ul << od);
                        }
                        rw[x] = diff_d(avg_l, df);
                        out[i + x*inner] = rw[x];
                    }

                }
                else {
                    if (input_nbits(icur, ibleft, 1)) {
                        for (uint32_t x=0; x<blk; x++) {
                            out[i + x*inner] = avg_l;
                        }
                        i += inner*blk;
                        continue;
                    }
                    else {
                        bas_l = 0;
                        for (uint32_t x=0; x < blk; x++) {
                            uint32_t od = input_cnt0p1(icur, ibleft), df;
                            if (!od) {
                                df = input_nbits(icur, ibleft, bas_l);
                            }
                            else {
                                od += bas_l - 1u;
                                df = input_nbits(icur, ibleft, od) + (1ul << od);
                            }
                            rw[x] = diff_d(avg_l, df);
                            out[i + x*inner] = rw[x];
                        }

                    }
                }
                

                for (uint32_t x = 0; x < blk; x++) {
                    mx = mx < rw[x] ? rw[x] : mx;
                    mn = mn > rw[x] ? rw[x] : mn;
                    avg_c += rw[x];
                }
                avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                if (mx == mn) {
                    bas_c = BW;
                }
                else {
                    memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                    for (uint32_t x = 0; x < blk; x++) {
                        bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                    }
                    for (uint32_t x = 0, sum = 0; x < BW; x++) {
                        sum += bkt[x];
                        if (sum >= blk - sum - bkt[x+1]) {
                            bas_c = x;
                            break;
                        }
                    }
                }

                bas_l = bas_c;
                avg_l = avg_c;
                i += inner*blk;
            }

        }
        //break;
    }


    free(bkt);
    free(rw);
    return k;
}

uint32_t fits_kcomp_u32(
    const dtype  in[],		    /* input array */
    const uint32_t iLen,		/* input length */
    uint8_t out[],	            /* output buffer */
    uint32_t oLen,		        /* max length of output */
    const uint32_t blk,         /* parameter that controls the block size */
    const uint32_t nsblk,       /* length of dimension with lowest variability	*/
    const uint32_t inner        /* the stride along this dimension */
) {
    uint32_t bigBlock = nsblk * inner;
    if (!bigBlock || iLen%bigBlock) {
        printf("error: dimensions don't match.\n");
        return 0;
    }
    if (blk < 16) {
        printf("error: blk should >= 16.\n");
        return 0;
    }
    if (nsblk < blk) {
        printf("error: nsblk should >= blk.\n");
        return 0;
    }
    if (iLen > (1u<<31)) {
        printf("warning: the input is too large.\n");
    }
    if (iLen + iLen/nsblk*(BW+LBW) > oLen) { 
        printf("warning: possible buffer overflow.\n");
    }// Under maliciously crafted input, the compress bound may reach iLen*2 + iLen/nsblk*(BW+LBW) + iLen/(16*blk)


    uint64_t* ocur = (uint64_t*)out;
    uint32_t obleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;
    *ocur = 0;

    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    dtype*    rw  = (dtype*)   aligned_alloc(64, sizeof(dtype)*lft);
    uint64_t* ci  = (uint64_t*)aligned_alloc(64, sizeof(uint64_t)*lft);
    uint32_t* li  = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*lft);

    if (!bkt || !rw || !ci || !li) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }


    for (uint32_t k=0; k<iLen; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) {
            uint32_t i=j, bas_l;
            uint64_t avg_l=0;
            dtype mx=0, mn=(1ul<<BW)-1u;


            for (uint32_t x = 0; x < lft; x++) {
                rw[x] = in[i + inner*x];
            }
            i += inner*lft;

            for (uint32_t x = 0; x < lft; x++) {
                mx = mx < rw[x] ? rw[x] : mx;
                mn = mn > rw[x] ? rw[x] : mn;
                avg_l += rw[x];
            }
            avg_l = (avg_l + lft - 3 - mn - mx) / (lft - 2);


            if (mx == mn) {
                bas_l = BW;
            }
            else {
                memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                for (uint32_t x = 0; x < lft; x++) {
                    bkt[sig_len(diff_e(avg_l, rw[x]))]++;
                }
                for (uint32_t x = 0, sum = 0; x < BW; x++) {
                    sum += bkt[x];
                    if (sum >= lft - sum - bkt[x+1]) {
                        bas_l = x;
                        break;
                    }
                }
            }


            if (bas_l < BW-1)
                output_nbits(ocur, obleft, bas_l, LBW);
            else if (bas_l == BW-1)
                output_nbits(ocur, obleft, (BW-1)+BW, LBW+1);
            else
                output_nbits(ocur, obleft, BW-1, LBW+1);
            


            if (bas_l != BW-1)
                output_nbits(ocur, obleft, avg_l, BW);

            if (bas_l < BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    ci[x] = diff_e(avg_l, rw[x]);
                    uint32_t m = sig_len(ci[x]>>bas_l);
                    ci[x] = ((ci[x]<<1u)|1u)<<m;
                    li[x] = bas_l + !m + m + m;
                    output_nbits_m64(ocur, obleft, ci[x], li[x]);
                }
            else if (bas_l == BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    output_nbits(ocur, obleft, rw[x], BW);
                }

            // ---
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c;
                uint64_t avg_c=0;
                mx=0, mn=(1ul<<BW)-1u;

                if (bas_l == BW-1) {  // random
                    for (uint32_t x = 0; x < blk; x++) {
                        rw[x] = in[i + inner*x];
                    }
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, rw[x], BW);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                    }
                }
                else if (bas_l == BW) {  // same
                    bas_l = 0;
                    for (uint32_t x = 0; x < blk; x++)
                        rw[x] = in[i + inner*x];
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);

                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    if (mx == mn && mx == avg_l) {
                        output_nbits(ocur, obleft, 1, 1);
                        avg_c = avg_l;
                        bas_c = BW;
                    }
                    else {
                        output_nbits(ocur, obleft, 0, 1);
                        avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                        if (mx == mn)
                            bas_c = BW;
                    }
                }
                else { // common
                    for (uint32_t x = 0; x < blk; x++)
                        rw[x] = in[i + inner*x];
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);
                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        for (uint32_t x = 0; x < blk; x++) {
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits_m64(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }

                    }

                }
                bas_l = bas_c;
                avg_l = avg_c;
                
            }
        }

    }

    done_outputing_bits(ocur, obleft);
    free(bkt);
    free(rw);
    free(ci);
    free(li);
    return (uint8_t*)ocur - out;
}
#undef BW
#undef LBW
#undef dtype
#undef diff_e
#undef diff_d
#undef sig_len

// -------------------------------------

#define dtype uint16_t
#define diff_e diff_e16
#define diff_d diff_d16
#define BW 16
#define LBW 4
#define sig_len(x) nonzero_count_u32(x)


uint32_t fits_kdecomp_u16(
    const uint8_t  in[],
    const uint32_t iLen,
    dtype  out[],
    uint32_t oLen,
    const uint32_t blk,
    const uint32_t nsblk,
    const uint32_t inner
) {
    uint32_t bigBlock = nsblk * inner;
    uint64_t* icur = (uint64_t*)in;
    uint32_t ibleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;

    dtype* rw = (dtype*)aligned_alloc(64, sizeof(dtype)*lft);
    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    if (!bkt || !rw) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }

    uint32_t k=0;
    for (; (uint8_t*)icur - in + (ibleft + 7) / 8 < iLen - 1; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) 
        {
            uint32_t i=j;
            uint32_t bas_l, avg_l;
            
            bas_l = input_nbits(icur, ibleft, LBW);
            
            if (bas_l == BW-1 && input_nbits(icur, ibleft, 1) == 0)
                bas_l = BW;          


            if (bas_l != BW-1) {
                avg_l = input_nbits(icur, ibleft, BW);
            }
                
            

            if (bas_l == BW) {
                for (uint32_t x=0; x < lft; x++)
                    out[i + x*inner] = avg_l;
            }
            else if (bas_l < BW-1) {
                for (uint32_t x=0; x < lft; x++) {
                    uint32_t od = input_cnt0p1(icur, ibleft), df;
                    if (!od) {
                        df = input_nbits(icur, ibleft, bas_l);
                    }
                    else {
                        od += bas_l - 1u;
                        df = input_nbits(icur, ibleft, od) + (1u << od);
                    }
                    out[i + x*inner] = diff_d(avg_l, df);
                }
            }
            else {
                for (uint32_t x=0; x < lft; x++) {
                    out[i + x*inner] = input_nbits(icur, ibleft, BW);
                }
            }
            i += inner*lft;
            

            
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c, avg_c=0;
                dtype mx=0, mn=(1u<<BW)-1u;

                if (bas_l == BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        rw[x] = input_nbits(icur, ibleft, BW);
                        out[i + x*inner] = rw[x];
                    }
                }
                else if (bas_l < BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        uint32_t od = input_cnt0p1(icur, ibleft), df;
                        if (!od) {
                            df = input_nbits(icur, ibleft, bas_l);
                        }
                        else {
                            od += bas_l - 1u;
                            df = input_nbits(icur, ibleft, od) + (1u << od);
                        }
                        rw[x] = diff_d(avg_l, df);
                        out[i + x*inner] = rw[x];
                    }

                }
                else {
                    if (input_nbits(icur, ibleft, 1)) {
                        for (uint32_t x=0; x<blk; x++) {
                            out[i + x*inner] = avg_l;
                        }
                        i += inner*blk;
                        continue;
                    }
                    else {
                        bas_l = 0;
                        for (uint32_t x=0; x < blk; x++) {
                            uint32_t od = input_cnt0p1(icur, ibleft), df;
                            if (!od) {
                                df = input_nbits(icur, ibleft, bas_l);
                            }
                            else {
                                od += bas_l - 1u;
                                df = input_nbits(icur, ibleft, od) + (1u << od);
                            }
                            rw[x] = diff_d(avg_l, df);
                            out[i + x*inner] = rw[x];
                        }

                    }
                }
                

                for (uint32_t x = 0; x < blk; x++) {
                    mx = mx < rw[x] ? rw[x] : mx;
                    mn = mn > rw[x] ? rw[x] : mn;
                    avg_c += rw[x];
                }
                avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                if (mx == mn) {
                    bas_c = BW;
                }
                else {
                    memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                    for (uint32_t x = 0; x < blk; x++) {
                        bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                    }
                    for (uint32_t x = 0, sum = 0; x < BW; x++) {
                        sum += bkt[x];
                        if (sum >= blk - sum - bkt[x+1]) {
                            bas_c = x;
                            break;
                        }
                    }
                }

                bas_l = bas_c;
                avg_l = avg_c;
                i += inner*blk;
            }

        }
        //break;
    }


    free(bkt);
    free(rw);
    return k;
}

uint32_t fits_kcomp_u16(
    const dtype  in[],		    /* input array */
    const uint32_t iLen,		/* input length */
    uint8_t out[],	            /* output buffer */
    uint32_t oLen,		        /* max length of output */
    const uint32_t blk,         /* parameter that controls the block size */
    const uint32_t nsblk,       /* length of dimension with lowest variability	*/
    const uint32_t inner        /* the stride along this dimension */
) {
    uint32_t bigBlock = nsblk * inner;
    if (!bigBlock || iLen%bigBlock) {
        printf("error: dimensions don't match.\n");
        return 0;
    }
    if (blk < 16) {
        printf("error: blk should >= 16.\n");
        return 0;
    }
    if (nsblk < blk) {
        printf("error: nsblk should >= blk.\n");
        return 0;
    }
    if (iLen > (1u<<31)) {
        printf("warning: the input is too large.\n");
    }
    if (iLen + iLen/nsblk*(BW+LBW) > oLen) { 
        printf("warning: possible buffer overflow.\n");
    }// Under maliciously crafted input, the compress bound may reach iLen*2 + iLen/nsblk*(BW+LBW) + iLen/(16*blk)


    uint64_t* ocur = (uint64_t*)out;
    uint32_t obleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;
    *ocur = 0;

    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    dtype*    rw  = (dtype*)   aligned_alloc(64, sizeof(dtype)*lft);
    uint32_t* ci  = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*lft);
    uint32_t* li  = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*lft);

    if (!bkt || !rw || !ci || !li) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }


    for (uint32_t k=0; k<iLen; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) {
            uint32_t i=j;
            uint32_t bas_l, avg_l=0;
            dtype mx=0, mn=(1u<<BW)-1u;


            for (uint32_t x = 0; x < lft; x++) {
                rw[x] = in[i + inner*x];
            }
            i += inner*lft;

            for (uint32_t x = 0; x < lft; x++) {
                mx = mx < rw[x] ? rw[x] : mx;
                mn = mn > rw[x] ? rw[x] : mn;
                avg_l += rw[x];
            }
            avg_l = (avg_l + lft - 3 - mn - mx) / (lft - 2);


            if (mx == mn) {
                bas_l = BW;
            }
            else {
                memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                for (uint32_t x = 0; x < lft; x++) {
                    bkt[sig_len(diff_e(avg_l, rw[x]))]++;
                }
                for (uint32_t x = 0, sum = 0; x < BW; x++) {
                    sum += bkt[x];
                    if (sum >= lft - sum - bkt[x+1]) {
                        bas_l = x;
                        break;
                    }
                }
            }


            if (bas_l < BW-1)
                output_nbits(ocur, obleft, bas_l, LBW);
            else if (bas_l == BW-1)
                output_nbits(ocur, obleft, (BW-1)+BW, LBW+1);
            else
                output_nbits(ocur, obleft, BW-1, LBW+1);
            
                

            if (bas_l != BW-1)
                output_nbits(ocur, obleft, avg_l, BW);

            if (bas_l < BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    ci[x] = diff_e(avg_l, rw[x]);
                    uint32_t m = sig_len(ci[x]>>bas_l);
                    ci[x] = ((ci[x]<<1u)|1u)<<m;
                    li[x] = bas_l + !m + m + m;
                    output_nbits(ocur, obleft, ci[x], li[x]);
                }
            else if (bas_l == BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    output_nbits(ocur, obleft, rw[x], BW);
                }

            // ---
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c, avg_c=0;
                mx=0, mn=(1u<<BW)-1u;

                if (bas_l == BW-1) {  // random
                    for (uint32_t x = 0; x < blk; x++) {
                        rw[x] = in[i + inner*x];
                    }
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, rw[x], BW);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                    }
                }
                else if (bas_l == BW) {  // same
                    bas_l = 0;
                    for (uint32_t x = 0; x < blk; x++)
                        rw[x] = in[i + inner*x];
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);

                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    if (mx == mn && mx == avg_l) {
                        output_nbits(ocur, obleft, 1, 1);
                        avg_c = avg_l;
                        bas_c = BW;
                    }
                    else {
                        output_nbits(ocur, obleft, 0, 1);
                        avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                        if (mx == mn)
                            bas_c = BW;
                    }
                }
                else { // common
                    for (uint32_t x = 0; x < blk; x++)
                        rw[x] = in[i + inner*x];
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);
                        ci[x] = ((ci[x]<<1u)|1u)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        for (uint32_t x = 0; x < blk; x++) {
                            output_nbits(ocur, obleft, ci[x], li[x]);
                        }
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }

                    }

                }
                bas_l = bas_c;
                avg_l = avg_c;
                
            }
        }

    }

    done_outputing_bits(ocur, obleft);
    free(bkt);
    free(rw);
    free(ci);
    free(li);
    return (uint8_t*)ocur - out;
}
#undef BW
#undef LBW
#undef dtype
#undef diff_e
#undef diff_d
#undef sig_len

// -------------------------------------

#define dtype uint8_t
#define diff_e diff_e8
#define diff_d diff_d8
#define BW 8
#define LBW 3
#define sig_len(x) nonzero_count_u8[x]


uint32_t fits_kdecomp_u8(
    const uint8_t  in[],
    const uint32_t iLen,
    dtype  out[],
    uint32_t oLen,
    const uint32_t blk,
    const uint32_t nsblk,
    const uint32_t inner
) {
    printf("HRAC_DEBUG: fits_kdecomp_u8 START. iLen=%u, oLen=%u, blk=%u, nsblk=%u\n", iLen, oLen, blk, nsblk);

    uint32_t bigBlock = nsblk * inner;
    uint64_t*  icur = (uint64_t*)in;
    uint32_t ibleft = 0;
    const uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;

    dtype* rw = (dtype*)aligned_alloc(64, sizeof(dtype)*lft);
    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    if (!bkt || !rw) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }

    uint32_t k=0;

    // 【调试日志】循环开始前
    printf("HRAC_DEBUG: Loop Start. bigBlock=%u\n", bigBlock);

    for (; (uint8_t*)icur - in + (ibleft + 7) / 8 < iLen - 1; k+=bigBlock) {
        
        // 【调试日志】进入外层循环
        printf("HRAC_DEBUG: Outer Loop k=%u. CurPos=%ld\n", k, (uint8_t*)icur - in);

        for (uint32_t j=k; j<k+inner; j++) 
        {
            uint32_t i=j;
            uint32_t bas_l, avg_l;
            
            bas_l = input_nbits(icur, ibleft, LBW);


            if (bas_l == BW-1 && input_nbits(icur, ibleft, 1) == 0)
                bas_l = BW;

            if (bas_l != BW-1)
                avg_l = input_nbits(icur, ibleft, BW);
            
            if (bas_l == BW) {
                for (uint32_t x=0; x < lft; x++) {
                    out[i + x*inner] = avg_l;
                }
            }
            else if (bas_l < BW-1) {
                for (uint32_t x=0; x < lft; x++) {
                    uint32_t od = input_cnt0p1(icur, ibleft), df;
                    if (!od) {
                        df = input_nbits(icur, ibleft, bas_l);
                    }
                    else {
                        od += bas_l - 1u;
                        df = input_nbits(icur, ibleft, od) + (1u << od);
                    }
                    out[i + x*inner] = diff_d(avg_l, df);
                }

            }
            else {
                for (uint32_t x=0; x < lft; x++) {
                    out[i + x*inner] = input_nbits(icur, ibleft, BW);
                }
            }
            i += inner*lft;


            
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c, avg_c=0;
                dtype mx=0, mn=(1u<<BW)-1u;

                if (bas_l == BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        rw[x] = input_nbits(icur, ibleft, BW);
                        out[i + x*inner] = rw[x];
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }
                }
                else if (bas_l < BW-1) {
                    for (uint32_t x=0; x < blk; x++) {
                        uint32_t od = input_cnt0p1(icur, ibleft), df;


                        if (!od) {
                            df = input_nbits(icur, ibleft, bas_l);
                        }
                        else {
                            od += bas_l - 1u;
                            df = input_nbits(icur, ibleft, od) + (1u << od);
                        }
                        rw[x] = diff_d(avg_l, df);
                        out[i + x*inner] = rw[x];
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                }
                else {
                    if (input_nbits(icur, ibleft, 1)) {
                        for (uint32_t x=0; x<blk; x++) {
                            out[i + x*inner] = avg_l;
                        }
                        i += inner*blk;
                        continue;
                    }
                    else {
                        bas_l = 0;
                        for (uint32_t x=0; x < blk; x++) {
                            uint32_t od = input_cnt0p1(icur, ibleft), df;
                            if (!od) {
                                df = 0;
                            }
                            else {
                                od--;
                                df = input_nbits(icur, ibleft, od) + (1u << od);
                            }
                            rw[x] = diff_d(avg_l, df);
                            out[i + x*inner] = rw[x];
                            mx = mx < rw[x] ? rw[x] : mx;
                            mn = mn > rw[x] ? rw[x] : mn;
                            avg_c += rw[x];
                        }

                    }
                }

                avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                if (mx == mn) {
                    bas_c = BW;
                }
                else {
                    memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                    
                    for (uint32_t x = 0; x < blk; x++) {
                        bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                    }
                    for (uint32_t x = 0, sum = 0; x < BW; x++) {
                        sum += bkt[x];
                        if (sum >= blk - sum - bkt[x+1]) {
                            bas_c = x;
                            break;
                        }
                    }
                }
                bas_l = bas_c;
                avg_l = avg_c;
                i += inner*blk;
            }

        }
        printf("HRAC_DEBUG: Break loop explicitly!\n");
        //break;
    }

    printf("HRAC_DEBUG: fits_kdecomp_u8 END. Return k=%u\n", k);
    free(bkt);
    free(rw);
    return k;
}

uint32_t fits_kcomp_u8(
    const dtype  in[],		    /* input array */
    const uint32_t iLen,		/* input length */
    uint8_t out[],	            /* output buffer */
    uint32_t oLen,		        /* max length of output */
    const uint32_t blk,         /* parameter that controls the block size */
    const uint32_t nsblk,       /* length of dimension with lowest variability	*/
    const uint32_t inner        /* the stride along this dimension */
) {
    uint32_t bigBlock = nsblk * inner;
    if (!bigBlock || iLen%bigBlock) {
        printf("error: dimensions don't match.\n");
        return 0;
    }
    if (blk < 16) {
        printf("error: blk should >= 16.\n");
        return 0;
    }
    if (nsblk < blk) {
        printf("error: nsblk should >= blk.\n");
        return 0;
    }
    if (iLen > (1u<<31)) {
        printf("warning: the input is too large.\n");
    }
    if (iLen + iLen/nsblk*(BW+LBW) > oLen) { 
        printf("warning: possible buffer overflow.\n");
    }// Under maliciously crafted input, the compress bound may reach iLen*2 + iLen/nsblk*(BW+LBW) + iLen/(16*blk)


    uint64_t* ocur = (uint64_t*)out;
    uint32_t obleft = 0;
    uint32_t scc = nsblk/blk - 1, lft = nsblk%blk + blk;
    *ocur = 0;

    uint32_t* bkt = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*(BW+1));
    dtype*    rw  = (dtype*)   aligned_alloc(64, sizeof(dtype)*lft);
    uint32_t* ci  = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*lft);
    uint32_t* li  = (uint32_t*)aligned_alloc(64, sizeof(uint32_t)*lft);

    if (!bkt || !rw || !ci || !li) {
        printf("error: aligned_alloc failed.\n");
        return 0;
    }


    for (uint32_t k=0; k<iLen; k+=bigBlock) {
        for (uint32_t j=k; j<k+inner; j++) {
            uint32_t i=j;
            uint32_t bas_l=100, avg_l=0;
            dtype mx=0, mn=(1u<<BW)-1u;


            for (uint32_t x = 0; x < lft; x++) {
                rw[x] = in[i + inner*x];
            }
                
            i += inner*lft;

            for (uint32_t x = 0; x < lft; x++) {
                mx = mx < rw[x] ? rw[x] : mx;
                mn = mn > rw[x] ? rw[x] : mn;
                avg_l += rw[x];
            }
            avg_l = (avg_l + lft - 3 - mn - mx) / (lft - 2);
            

            if (mx == mn) {
                bas_l = BW;
            }
            else {
                memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                for (uint32_t x = 0; x < lft; x++) {
                    bkt[sig_len(diff_e(avg_l, rw[x]))]++;
                }
                for (uint32_t x = 0, sum = 0; x < BW; x++) {
                    sum += bkt[x];
                    if (sum >= lft - sum - bkt[x+1]) {
                        bas_l = x;
                        break;
                    }
                }
            }


            if (bas_l < BW-1)
                output_nbits(ocur, obleft, bas_l, LBW);
            else if (bas_l == BW-1)
                output_nbits(ocur, obleft, (BW-1)+BW, LBW+1);
            else 
                output_nbits(ocur, obleft, BW-1, LBW+1);
                

            if (bas_l != BW-1)
                output_nbits(ocur, obleft, avg_l, BW);

            if (bas_l < BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    ci[x] = diff_e(avg_l, rw[x]);
                    uint32_t m = sig_len(ci[x]>>bas_l);
                    ci[x] = ((ci[x]<<1)|1)<<m;
                    li[x] = bas_l + !m + m + m;
                    output_nbits(ocur, obleft, ci[x], li[x]);
                }
            else if (bas_l == BW-1)
                for (uint32_t x = 0; x < lft; x++) {
                    output_nbits(ocur, obleft, rw[x], BW);
                }

            // ---
            for (uint32_t _ = 0; _ < scc; _++) {
                uint32_t bas_c=100, avg_c=0;
                mx=0, mn=(1u<<BW)-1u;

                if (bas_l == BW-1) {  // random
                    for (uint32_t x = 0; x < blk; x++) {
                        rw[x] = in[i + inner*x];
                    }
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        output_nbits(ocur, obleft, rw[x], BW);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                    }
                }
                else if (bas_l == BW) {  // same
                    bas_l = 0;
                    for (uint32_t x = 0; x < blk; x++)
                        rw[x] = in[i + inner*x];
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                    }
                    for (uint32_t x = 0; x < blk; x++) {
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);
                        ci[x] = ((ci[x]<<1)|1)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    if (mx == mn && mx == avg_l) {
                        output_nbits(ocur, obleft, 1, 1);
                        avg_c = avg_l;
                        bas_c = BW;
                    }
                    else {
                        output_nbits(ocur, obleft, 0, 1);
                        avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }
                        if (mx == mn)
                            bas_c = BW;
                    }
                }
                else { // common
                    for (uint32_t x = 0; x < blk; x++)
                        rw[x] = in[i + inner*x];
                    i += inner*blk;

                    for (uint32_t x = 0; x < blk; x++) {
                        ci[x] = diff_e(avg_l, rw[x]);
                        mx = mx < rw[x] ? rw[x] : mx;
                        mn = mn > rw[x] ? rw[x] : mn;
                        avg_c += rw[x];
                    }

                    for (uint32_t x = 0; x < blk; x++) {
                        uint32_t m = sig_len(ci[x]>>bas_l);
                        ci[x] = ((ci[x]<<1)|1)<<m;
                        li[x] = bas_l + !m + m + m;
                    }

                    avg_c = (avg_c + blk - 3 - mn - mx) / (blk - 2);

                    if (mx == mn) {
                        for (uint32_t x = 0; x < blk; x++) {
                            output_nbits(ocur, obleft, ci[x], li[x]);
                        }
                        bas_c = BW;
                    }
                    else {
                        memset(bkt, 0, sizeof(uint32_t)*(BW+1));
                        for (uint32_t x = 0; x < blk; x++) {
                            bkt[sig_len(diff_e(avg_c, rw[x]))]++;
                            output_nbits(ocur, obleft, ci[x], li[x]);
                        }
                        for (uint32_t x = 0, sum = 0; x < BW; x++) {
                            sum += bkt[x];
                            if (sum >= blk - sum - bkt[x+1]) {
                                bas_c = x;
                                break;
                            }
                        }

                    }

                }
                bas_l = bas_c;
                avg_l = avg_c;
                
            }
        }

    }

    done_outputing_bits(ocur, obleft);
    free(bkt);
    free(rw);
    free(ci);
    free(li);
    return (uint8_t*)ocur - out;
}
#undef BW
#undef LBW
#undef dtype
#undef diff_e
#undef diff_d
#undef sig_len
