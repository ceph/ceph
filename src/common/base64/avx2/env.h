
#ifndef BASE64_ENV_H
#define BASE64_ENV_H

// Define machine endianness. This is for GCC:
#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#  define BASE64_LITTLE_ENDIAN 1
#else
#  define BASE64_LITTLE_ENDIAN 0
#endif

// This is for Clang:
#ifdef __LITTLE_ENDIAN__
#  define BASE64_LITTLE_ENDIAN 1
#endif

#ifdef __BIG_ENDIAN__
#  define BASE64_LITTLE_ENDIAN 0
#endif

// Endian conversion functions:
#if BASE64_LITTLE_ENDIAN
//   GCC and Clang:
#    define BASE64_HTOBE32(x)	__builtin_bswap32(x)
#    define BASE64_HTOBE64(x)	__builtin_bswap64(x)
#else
// No conversion needed:
#  define BASE64_HTOBE32(x)	(x)
#  define BASE64_HTOBE64(x)	(x)
#endif

#define BASE64_FALLTHROUGH __attribute__((fallthrough));

// Detect word size:
#ifdef _INTEGRAL_MAX_BITS
#  define BASE64_WORDSIZE _INTEGRAL_MAX_BITS
#else
#  define BASE64_WORDSIZE __WORDSIZE
#endif

// End-of-file definitions.
// Almost end-of-file when waiting for the last '=' character:
#define BASE64_AEOF 1
// End-of-file when stream end has been reached or invalid input provided:
#define BASE64_EOF 2

#endif	// BASE64_ENV_H
