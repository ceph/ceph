
/**
 * Copyright (c) 2016 Tino Reichardt
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 * You can contact the author at:
 * - zstdmt source repository: https://github.com/mcmilk/zstdmt
 */

/* ***************************************
 * Defines
 ****************************************/

#ifndef ZSTDMT_H
#define ZSTDMT_H

#if defined (__cplusplus)
extern "C" {
#endif

#include <stddef.h>   /* size_t */

#define ZSTDMT_THREAD_MAX 128
#define ZSTDMT_LEVEL_MIN    1
#define ZSTDMT_LEVEL_MAX   22

/* zstd magic values */
#define ZSTDMT_MAGICNUMBER_V01  0x1EB52FFDU
#define ZSTDMT_MAGICNUMBER_MIN  0xFD2FB522U
#define ZSTDMT_MAGICNUMBER_MAX  0xFD2FB528U
#define ZSTDMT_MAGIC_SKIPPABLE  0x184D2A50U

/* **************************************
 * Error Handling
 ****************************************/

typedef enum {
  ZSTDMT_error_no_error,
  ZSTDMT_error_memory_allocation,
  ZSTDMT_error_init_missing,
  ZSTDMT_error_read_fail,
  ZSTDMT_error_write_fail,
  ZSTDMT_error_data_error,
  ZSTDMT_error_frame_compress,
  ZSTDMT_error_frame_decompress,
  ZSTDMT_error_compressionParameter_unsupported,
  ZSTDMT_error_compression_library,
  ZSTDMT_error_canceled,
  ZSTDMT_error_maxCode
} ZSTDMT_ErrorCode;

extern size_t zstdmt_errcode;

#define ZSTDMT_PREFIX(name) ZSTDMT_error_##name
#define ZSTDMT_ERROR(name) ((size_t)-ZSTDMT_PREFIX(name))
extern unsigned ZSTDMT_isError(size_t code);
extern const char* ZSTDMT_getErrorString(size_t code);

/* **************************************
 * Structures
 ****************************************/

typedef struct {
  void *buf;		/* ptr to data */
  size_t size;		/* current filled in buf */
  size_t allocated;	/* length of buf */
} ZSTDMT_Buffer;

/**
 * reading and writing functions
 * - you can use stdio functions or plain read/write
 * - just write some wrapper on your own
 * - a sample is given in 7-Zip ZS
 *
 * error definitions:
 *  0 = success
 * -1 = generic read/write error
 * -2 = user abort
 * -3 = memory
 */
typedef int (fn_read) (void *args, ZSTDMT_Buffer * in);
typedef int (fn_write) (void *args, ZSTDMT_Buffer * out);

typedef struct {
  fn_read *fn_read;
  void *arg_read;
  fn_write *fn_write;
  void *arg_write;
} ZSTDMT_RdWr_t;

/* **************************************
 * Compression
 ****************************************/

typedef struct ZSTDMT_CCtx_s ZSTDMT_CCtx;

/**
 * ZSTDMT_createCCtx() - allocate new compression context
 *
 * This function allocates and initializes an zstd commpression context.
 * The context can be used multiple times without the need for resetting
 * or re-initializing.
 *
 * @level: compression level, which should be used (1..22)
 * @threads: number of threads, which should be used (1..ZSTDMT_THREAD_MAX)
 * @inputsize: - if zero, becomes some optimal value for the level
 *             - if nonzero, the given value is taken
 * @zstdmt_errcode: space for storing zstd errors (needed for thread safety)
 * @return: the context on success, zero on error
 */
ZSTDMT_CCtx *ZSTDMT_createCCtx(int threads, int level, int inputsize);

/**
 * ZSTDMT_compressDCtx() - threaded compression for zstd
 *
  * This function will create valid zstd streams. The number of threads,
 * the input chunksize and the compression level are ....
 *
 * @ctx: context, which needs to be created with ZSTDMT_createDCtx()
 * @rdwr: callback structure, which defines reding/writing functions
 * @return: zero on success, or error code
 */
size_t ZSTDMT_compressCCtx(ZSTDMT_CCtx * ctx, ZSTDMT_RdWr_t * rdwr);

/**
 * ZSTDMT_GetFramesCCtx() - number of written frames
 * ZSTDMT_GetInsizeCCtx() - read bytes of input
 * ZSTDMT_GetOutsizeCCtx() - written bytes of output
 *
 * These three functions will return some statistical data of the
 * compression context ctx.
 *
 * @ctx: context, which should be examined
 * @return: the request value, or zero on error
 */
size_t ZSTDMT_GetFramesCCtx(ZSTDMT_CCtx * ctx);
size_t ZSTDMT_GetInsizeCCtx(ZSTDMT_CCtx * ctx);
size_t ZSTDMT_GetOutsizeCCtx(ZSTDMT_CCtx * ctx);

/**
 * ZSTDMT_freeCCtx() - free compression context
 *
 * This function will free all allocated resources, which were allocated
 * by ZSTDMT_createCCtx(). This function can not fail.
 *
 * @ctx: context, which should be freed
 */
void ZSTDMT_freeCCtx(ZSTDMT_CCtx * ctx);

/* **************************************
 * Decompression
 ****************************************/

typedef struct ZSTDMT_DCtx_s ZSTDMT_DCtx;

/**
 * 1) allocate new cctx
 * - return cctx or zero on error
 *
 * @level   - 1 .. 22
 * @threads - 1 .. ZSTDMT_THREAD_MAX
 * @srclen  - the max size of src for ZSTDMT_compressCCtx()
 * @dstlen  - the min size of dst
 */
ZSTDMT_DCtx *ZSTDMT_createDCtx(int threads, int inputsize);

/**
 * ZSTDMT_decompressDCtx() - threaded decompression for zstd
 *
 * This function will decompress valid zstd streams.
 *
 * @ctx: context, which needs to be created with ZSTDMT_createDCtx()
 * @rdwr: callback structure, which defines reding/writing functions
 * @return: zero on success, or error code
 */
size_t ZSTDMT_decompressDCtx(ZSTDMT_DCtx * ctx, ZSTDMT_RdWr_t * rdwr);

/**
 * ZSTDMT_GetFramesDCtx() - number of read frames
 * ZSTDMT_GetInsizeDCtx() - read bytes of input
 * ZSTDMT_GetOutsizeDCtx() - written bytes of output
 *
 * These three functions will return some statistical data of the
 * decompression context ctx.
 *
 * @ctx: context, which should be examined
 * @return: the request value, or zero on error
 */
size_t ZSTDMT_GetFramesDCtx(ZSTDMT_DCtx * ctx);
size_t ZSTDMT_GetInsizeDCtx(ZSTDMT_DCtx * ctx);
size_t ZSTDMT_GetOutsizeDCtx(ZSTDMT_DCtx * ctx);

/**
 * ZSTDMT_freeDCtx() - free decompression context
 *
 * This function will free all allocated resources, which were allocated
 * by ZSTDMT_createDCtx(). This function can not fail.
 *
 * @ctx: context, which should be freed
 */
void ZSTDMT_freeDCtx(ZSTDMT_DCtx * ctx);

#if defined (__cplusplus)
}
#endif
#endif				/* ZSTDMT_H */
