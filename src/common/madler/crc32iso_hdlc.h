// The _bit, _byte, and _word routines return the CRC of the len
// bytes at mem, applied to the previous CRC value, crc. If mem is
// NULL, then the other arguments are ignored, and the initial CRC,
// i.e. the CRC of zero bytes, is returned. Those routines will all
// return the same result, differing only in speed and code
// complexity. The _rem routine returns the CRC of the remaining
// bits in the last byte, for when the number of bits in the
// message is not a multiple of eight. The low bits bits of the low
// byte of val are applied to crc. bits must be in 0..8.

#include <stddef.h>
#include <stdint.h>

// Compute the CRC a bit at a time.
uint32_t crc32iso_hdlc_bit(uint32_t crc, void const *mem, size_t len);

// Compute the CRC of the low bits bits in val.
uint32_t crc32iso_hdlc_rem(uint32_t crc, unsigned val, unsigned bits);

// Compute the CRC a byte at a time.
uint32_t crc32iso_hdlc_byte(uint32_t crc, void const *mem, size_t len);

// Compute the CRC a word at a time.
uint32_t crc32iso_hdlc_word(uint32_t crc, void const *mem, size_t len);

// Compute the combination of two CRCs.
uint32_t crc32iso_hdlc_comb(uint32_t crc1, uint32_t crc2, uintmax_t len2);
