#ifndef HRAC_H
#define HRAC_H

#include <cstdint>

/* 
    dtype1 in[],		    	// input array
    const uint32_t iLen,		// input length
    dtype2 out[],	            // output buffer
    uint32_t oLen,		        // max length of output
    const uint32_t blk,         // parameter that controls the block size
    const uint32_t nsblk,       // length of dimension with lowest variability
    const uint32_t inner        // the stride along this dimension
*/

uint32_t fits_kdecomp_f64(const uint8_t  in[],  const uint32_t iLen, double   out_[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);
uint32_t fits_kcomp_f64  (const double   in_[], const uint32_t iLen, uint8_t  out[],  uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);

uint32_t fits_kdecomp_f32(const uint8_t  in[],  const uint32_t iLen, float    out_[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);
uint32_t fits_kcomp_f32  (const float    in_[], const uint32_t iLen, uint8_t  out[],  uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);

uint32_t fits_kdecomp_u32(const uint8_t   in[], const uint32_t iLen, uint32_t  out[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);
uint32_t fits_kcomp_u32  (const uint32_t  in[], const uint32_t iLen, uint8_t   out[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);

uint32_t fits_kdecomp_u16(const uint8_t   in[], const uint32_t iLen, uint16_t  out[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);
uint32_t fits_kcomp_u16  (const uint16_t  in[], const uint32_t iLen, uint8_t   out[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);

uint32_t fits_kdecomp_u8(const uint8_t    in[], const uint32_t iLen, uint8_t   out[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);
uint32_t fits_kcomp_u8  (const uint8_t    in[], const uint32_t iLen, uint8_t   out[], uint32_t oLen, const uint32_t blk, const uint32_t nsblk, const uint32_t inner);


#endif // HRAC_H
