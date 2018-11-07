
/**
 * Copyright (c) 2016 - 2017 Tino Reichardt
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 * You can contact the author at:
 * - zstdmt source repository: https://github.com/mcmilk/zstdmt
 */

#include <stdlib.h>
#include <string.h>

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"

#include "memmt.h"
#include "threading.h"
#include "list.h"
#include "zstd-mt.h"

/**
 * multi threaded zstd decompression
 *
 * - each thread works on his own
 * - needs a callback for reading / writing
 * - each worker does this:
 *   1) get read mutex and read some input
 *   2) release read mutex and do decompression
 *   3) get write mutex and write result
 *   4) begin with step 1 again, until no input
 */

#if 0
#include <stdio.h>
#define dprintf(fmt, arg...) do { printf(fmt, ## arg); } while (0)
#else
#define dprintf(fmt, ...)
#endif				/* DEBUG */

extern size_t zstdmt_errcode;

/* worker for compression */
typedef struct {
  ZSTDMT_DCtx *ctx;
  pthread_t pthread;
  ZSTDMT_Buffer in;
  ZSTD_DStream *dctx;
} cwork_t;

struct writelist;
struct writelist {
  size_t frame;
  ZSTDMT_Buffer out;
  struct list_head node;
};

struct ZSTDMT_DCtx_s {

  /* threads: 1..ZSTDMT_THREAD_MAX */
  int threads;
  int threadswanted;

  /* input buffer, used at single threading */
  size_t inputsize;

  /* buffersize used for output */
  size_t outputsize;

  /* statistic */
  size_t insize;
  size_t outsize;
  size_t curframe;
  size_t frames;

  /* threading */
  cwork_t *cwork;

  /* reading input */
  pthread_mutex_t read_mutex;
  fn_read *fn_read;
  void *arg_read;

  /* writing output */
  pthread_mutex_t write_mutex;
  fn_write *fn_write;
  void *arg_write;

  /* error handling */
  pthread_mutex_t error_mutex;

  /* lists for writing queue */
  struct list_head writelist_free;
  struct list_head writelist_busy;
  struct list_head writelist_done;
};

/* **************************************
 * Decompression
 ****************************************/

ZSTDMT_DCtx *ZSTDMT_createDCtx(int threads, int inputsize)
{
  ZSTDMT_DCtx *ctx;

  /* allocate ctx */
  ctx = (ZSTDMT_DCtx *) malloc(sizeof(ZSTDMT_DCtx));
  if (!ctx)
    return 0;

  /* check threads value */
  if (threads < 1 || threads > ZSTDMT_THREAD_MAX)
    return 0;

  /* setup ctx */
  ctx->threadswanted = threads;
  ctx->threads = 0;
  ctx->insize = 0;
  ctx->outsize = 0;
  ctx->frames = 0;
  ctx->curframe = 0;

  /* will be used for single stream only */
  if (inputsize)
    ctx->inputsize = inputsize;
  else
    ctx->inputsize = 1024 * 512;

  /* frame size (will get higher, when needed) */
  ctx->outputsize = 1024 * 512;

  /* later */
  ctx->cwork = 0;

  return ctx;
}

/**
 * IsZstd_Magic - check, if 4 bytes are valid ZSTD MAGIC
 */
static int IsZstd_Magic(unsigned char *buf)
{
  U32 magic = MEM_readLE32(buf);
  if (magic == ZSTDMT_MAGICNUMBER_V01)
    return 1;
  return (magic >= ZSTDMT_MAGICNUMBER_MIN
          && magic <= ZSTDMT_MAGICNUMBER_MAX);
}

/**
 * IsZstd_Skippable - check, if 4 bytes are MAGIC_SKIPPABLE
 */
static int IsZstd_Skippable(unsigned char *buf)
{
  return (MEM_readLE32(buf) == ZSTDMT_MAGIC_SKIPPABLE);
}

/**
 * mt_error - return mt lib specific error code
 */
static size_t mt_error(int rv)
{
  switch (rv) {
    case -1:
      return ZSTDMT_ERROR(read_fail);
    case -2:
      return ZSTDMT_ERROR(canceled);
    case -3:
      return ZSTDMT_ERROR(memory_allocation);
  }

  /* XXX, some catch all other errors */
  return ZSTDMT_ERROR(read_fail);
}

/**
 * pt_write - queue for decompressed output
 */
static size_t pt_write(ZSTDMT_DCtx * ctx, struct writelist *wl)
{
  struct list_head *entry;

  /* move the entry to the done list */
  list_move(&wl->node, &ctx->writelist_done);
  again:
  /* check, what can be written ... */
  list_for_each(entry, &ctx->writelist_done) {
    wl = list_entry(entry, struct writelist, node);
    if (wl->frame == ctx->curframe) {
      int rv = ctx->fn_write(ctx->arg_write, &wl->out);
      if (rv != 0)
        return mt_error(rv);
      ctx->outsize += wl->out.size;
      ctx->curframe++;
      list_move(entry, &ctx->writelist_free);
      goto again;
    }
  }

  return 0;
}

/**
 * pt_read - read compressed input
 */
static size_t pt_read(ZSTDMT_DCtx * ctx, ZSTDMT_Buffer * in, size_t * frame)
{
  unsigned char hdrbuf[12];
  ZSTDMT_Buffer hdr;
  size_t toRead;
  int rv;

  pthread_mutex_lock(&ctx->read_mutex);

  /* special case, some bytes were read by magic check */
  if (unlikely(ctx->frames == 0)) {
    /* the magic check reads exactly 16 bytes! */
    if (unlikely(in->size != 16))
      goto error_data;
    ctx->insize += 16;

    /**
     * zstdmt mode, with zstd magic prefix
     * 9 bytes zero byte frame + 12 byte skippable
     * - 21 bytes to read, 16 bytes done
     * - read 5 bytes, put them together (12 byte hdr)
     */
    if (!IsZstd_Skippable(in->buf)) {
      memcpy(hdrbuf, in->buf, 7);
      hdr.buf = hdrbuf + 7;
      hdr.size = 5;
      rv = ctx->fn_read(ctx->arg_read, &hdr);
      if (rv != 0) {
        pthread_mutex_unlock(&ctx->read_mutex);
        return mt_error(rv);
      }
      if (hdr.size != 5)
        goto error_data;
      hdr.buf = hdrbuf;
      ctx->insize += 16 + 5;

      /* read data */
      toRead = MEM_readLE32((unsigned char *)hdr.buf + 8);
      in->size = toRead;
      in->buf = malloc(in->size);
      if (!in->buf)
        goto error_nomem;
      in->allocated = in->size;
      rv = ctx->fn_read(ctx->arg_read, in);
      if (rv != 0) {
        pthread_mutex_unlock(&ctx->read_mutex);
        return mt_error(rv);
      }
      if (in->size != toRead)
        goto error_data;
      ctx->insize += in->size;
      *frame = ctx->frames++;
      pthread_mutex_unlock(&ctx->read_mutex);
      return 0;	/* done! */
    }

    /**
     * pzstd mode, no prefix
     * - start directly with 12 byte skippable frame
     */
    if (IsZstd_Skippable(in->buf)) {
      unsigned char *start = in->buf;	/* 16 bytes data */
      toRead = MEM_readLE32((unsigned char *)start + 8);
      in->size = toRead;
      in->buf = malloc(in->size);
      if (!in->buf)
        goto error_nomem;
      in->allocated = in->size;
      /* copy 4 bytes user data to new buf */
      memcpy(in->buf, start + 12, 4);
      start = in->buf;	/* point to in->buf now */

      /* 12 byte skippable, so 4 bytes data done */
      in->buf = start + 4;
      in->size = toRead - 4;
      rv = ctx->fn_read(ctx->arg_read, in);
      if (rv != 0) {
        pthread_mutex_unlock(&ctx->read_mutex);
        return mt_error(rv);
      }
      if (in->size != toRead - 4)
        goto error_data;
      ctx->insize += in->size;
      in->buf = start;	/* restore inbuf */
      in->size += 4;
      *frame = ctx->frames++;
      pthread_mutex_unlock(&ctx->read_mutex);
      return 0;	/* done! */
    }
  }

  /**
   * read next skippable frame (12 bytes)
   * 4 bytes skippable magic
   * 4 bytes little endian, must be: 4 (user data size)
   * 4 bytes little endian, size to read (user data)
   */
  hdr.buf = hdrbuf;
  hdr.size = 12;
  rv = ctx->fn_read(ctx->arg_read, &hdr);
  if (rv != 0) {
    pthread_mutex_unlock(&ctx->read_mutex);
    return mt_error(rv);
  }

  /* eof reached ? */
  if (unlikely(hdr.size == 0)) {
    pthread_mutex_unlock(&ctx->read_mutex);
    in->size = 0;
    return 0;
  }

  /* check header data */
  if (unlikely(hdr.size != 12))
    goto error_read;
  if (unlikely(!IsZstd_Skippable(hdr.buf)))
    goto error_data;
  ctx->insize += 12;

  /* read new input (size should be _toRead_ bytes */
  toRead = MEM_readLE32((unsigned char *)hdr.buf + 8);
  {
    if (in->allocated < toRead) {
      /* need bigger input buffer */
      if (in->allocated)
        in->buf = realloc(in->buf, toRead);
      else
        in->buf = malloc(toRead);
      if (!in->buf)
        goto error_nomem;
      in->allocated = toRead;
    }

    in->size = toRead;
    rv = ctx->fn_read(ctx->arg_read, in);
    if (rv != 0) {
      pthread_mutex_unlock(&ctx->read_mutex);
      return mt_error(rv);
    }
    /* needed more bytes! */
    if (in->size != toRead)
      goto error_data;

    ctx->insize += in->size;
  }
  *frame = ctx->frames++;
  pthread_mutex_unlock(&ctx->read_mutex);

  /* done, no error */
  return 0;

  error_data:
  pthread_mutex_unlock(&ctx->read_mutex);
  return ZSTDMT_ERROR(data_error);
  error_read:
  pthread_mutex_unlock(&ctx->read_mutex);
  return ZSTDMT_ERROR(read_fail);
  error_nomem:
  pthread_mutex_unlock(&ctx->read_mutex);
  return ZSTDMT_ERROR(memory_allocation);
}

static void *pt_decompress(void *arg)
{
  cwork_t *w = (cwork_t *) arg;
  ZSTDMT_Buffer *in = &w->in;
  ZSTDMT_DCtx *ctx = w->ctx;
  struct writelist *wl;
  size_t result = 0;
  ZSTDMT_Buffer collect;

  /* init dstream stream */
  result = ZSTD_initDStream(w->dctx);
  if (ZSTD_isError(result)) {
    zstdmt_errcode = result;
    return (void *)ZSTDMT_ERROR(compression_library);
  }

  collect.buf = 0;
  collect.size = 0;
  collect.allocated = 0;
  for (;;) {
    ZSTDMT_Buffer *out;
    ZSTD_inBuffer zIn;
    ZSTD_outBuffer zOut;

    /* select or allocate space for new output */
    pthread_mutex_lock(&ctx->write_mutex);
    if (!list_empty(&ctx->writelist_free)) {
      /* take unused entry */
      struct list_head *entry;
      entry = list_first(&ctx->writelist_free);
      wl = list_entry(entry, struct writelist, node);
      list_move(entry, &ctx->writelist_busy);
    } else {
      /* allocate new one */
      wl = (struct writelist *)
              malloc(sizeof(struct writelist));
      if (!wl) {
        result = ZSTDMT_ERROR(memory_allocation);
        goto error_unlock;
      }
      out = &wl->out;
      out->size = ctx->outputsize;
      out->buf = malloc(out->size);
      if (!out->buf) {
        result = ZSTDMT_ERROR(memory_allocation);
        goto error_unlock;
      }
      out->allocated = out->size;
      list_add(&wl->node, &ctx->writelist_busy);
    }

    /* start with 512KB */
    /* XXX, add framesize detection... */
    out = &wl->out;
    pthread_mutex_unlock(&ctx->write_mutex);

    /* init dstream stream */
    result = ZSTD_resetDStream(w->dctx);
    if (ZSTD_isError(result)) {
      zstdmt_errcode = result;
      return (void *)ZSTDMT_ERROR(compression_library);
    }

    /* zero should not happen here! */
    result = pt_read(ctx, in, &wl->frame);
    if (in->size == 0)
      break;
    if (ZSTDMT_isError(result)) {
      goto error_lock;
    }

    zIn.size = in->allocated;
    zIn.src = in->buf;
    zIn.pos = 0;

    for (;;) {
      again:
      /* decompress loop */
      zOut.size = out->allocated;
      zOut.dst = out->buf;
      zOut.pos = 0;

      dprintf
      ("ZSTD_decompressStream() zIn.size=%zu zIn.pos=%zu zOut.size=%zu zOut.pos=%zu\n",
       zIn.size, zIn.pos, zOut.size, zOut.pos);
      result = ZSTD_decompressStream(w->dctx, &zOut, &zIn);
      dprintf
      ("ZSTD_decompressStream(), ret=%zu zIn.size=%zu zIn.pos=%zu zOut.size=%zu zOut.pos=%zu\n",
       result, zIn.size, zIn.pos, zOut.size, zOut.pos);
      if (ZSTD_isError(result))
        goto error_clib;

      /* end of frame */
      if (result == 0) {
        /* put collected stuff together */
        if (collect.size) {
          void *bnew;
          bnew = malloc(collect.size + zOut.pos);
          if (!bnew) {
            result =
                    ZSTDMT_ERROR
                    (memory_allocation);
            goto error_lock;
          }
          memcpy((char *)bnew, collect.buf,
                 collect.size);
          memcpy((char *)bnew + collect.size,
                 out->buf, zOut.pos);
          free(collect.buf);
          free(out->buf);
          out->buf = bnew;
          out->size = collect.size + zOut.pos;
          out->allocated = out->size;
          collect.buf = 0;
          collect.size = 0;
        } else {
          out->size = zOut.pos;
        }
        /* write result */
        pthread_mutex_lock(&ctx->write_mutex);
        result = pt_write(ctx, wl);
        if (ZSTDMT_isError(result))
          goto error_unlock;
        pthread_mutex_unlock(&ctx->write_mutex);
        /* will read next input */
        break;
      }

      /* out buffer to small for full frame */
      if (result != 0) {
        /* collect old content from out */
        collect.buf =
                realloc(collect.buf,
                        collect.size + out->size);
        memcpy((char *)collect.buf + collect.size,
               out->buf, out->size);
        collect.size = collect.size + out->size;

        /* double the buffer, until it fits */
        pthread_mutex_lock(&ctx->write_mutex);
        out->size *= 2;
        ctx->outputsize = out->size;
        pthread_mutex_unlock(&ctx->write_mutex);
        out->buf = realloc(out->buf, out->size);
        if (!out->buf) {
          result =
                  ZSTDMT_ERROR(memory_allocation);
          goto error_lock;
        }
        out->allocated = out->size;
        goto again;
      }

      if (zIn.pos == zIn.size)
        break;	/* should fail... */
    }		/* decompress loop */
  }			/* read input loop */

  /* everything is okay */
  pthread_mutex_lock(&ctx->write_mutex);
  list_move(&wl->node, &ctx->writelist_free);
  pthread_mutex_unlock(&ctx->write_mutex);
  if (in->allocated)
    free(in->buf);
  return 0;

  error_clib:
  zstdmt_errcode = result;
  result = ZSTDMT_ERROR(compression_library);
  /* fall through */
  error_lock:
  pthread_mutex_lock(&ctx->write_mutex);
  error_unlock:
  list_move(&wl->node, &ctx->writelist_free);
  pthread_mutex_unlock(&ctx->write_mutex);
  if (in->allocated)
    free(in->buf);
  return (void *)result;
}

/* single threaded */
static size_t st_decompress(void *arg)
{
  ZSTDMT_DCtx *ctx = (ZSTDMT_DCtx *) arg;
  cwork_t *w = &ctx->cwork[0];
  ZSTDMT_Buffer In, Out;
  ZSTDMT_Buffer *in = &In;
  ZSTDMT_Buffer *out = &Out;
  ZSTDMT_Buffer *magic = &w->in;
  size_t result;
  int rv;

  ZSTD_inBuffer zIn;
  ZSTD_outBuffer zOut;

  /* init dstream stream */
  result = ZSTD_initDStream(w->dctx);
  if (ZSTD_isError(result)) {
    zstdmt_errcode = result;
    return ZSTDMT_ERROR(compression_library);
  }

  /* allocate space for input buffer */
  in->size = ZSTD_DStreamInSize();
  in->buf = malloc(in->size);
  if (!in->buf)
    return ZSTDMT_ERROR(memory_allocation);
  in->allocated = in->size;

  /* allocate space for output buffer */
  out->size = ZSTD_DStreamOutSize();
  out->buf = malloc(out->size);
  if (!out->buf) {
    free(in->buf);
    return ZSTDMT_ERROR(memory_allocation);
  }
  out->allocated = out->size;

  /* we read already some bytes, handle that: */
  {
    /* remember in->buf */
    unsigned char *buf = in->buf;

    /* fill first read bytes to buffer... */
    memcpy(in->buf, magic->buf, magic->size);
    magic->buf = in->buf;
    in->buf = buf + magic->size;
    in->size = in->allocated - magic->size;

    /* read more bytes, to fill buffer */
    rv = ctx->fn_read(ctx->arg_read, in);
    if (rv != 0) {
      result = mt_error(rv);
      goto error;
    }

    /* ready, first buffer complete */
    in->buf = buf;
    in->size += magic->size;
    ctx->insize += in->size;
  }

  zIn.src = in->buf;
  zIn.size = in->size;
  zIn.pos = 0;

  zOut.dst = out->buf;

  for (;;) {
    for (;;) {
      /* decompress loop */
      zOut.size = out->allocated;
      zOut.pos = 0;

      result = ZSTD_decompressStream(w->dctx, &zOut, &zIn);
      if (ZSTD_isError(result))
        goto error_clib;

      if (zOut.pos) {
        ZSTDMT_Buffer wb;
        wb.size = zOut.pos;
        wb.buf = zOut.dst;
        rv = ctx->fn_write(ctx->arg_write, &wb);
        if (rv != 0) {
          result = mt_error(rv);
          goto error;
        }
        ctx->outsize += zOut.pos;
      }

      /* one more round */
      if ((zIn.pos == zIn.size) && (result == 1) && zOut.pos)
        continue;

      /* finished */
      if (zIn.pos == zIn.size)
        break;

      /* end of frame */
      if (result == 0) {
        result = ZSTD_resetDStream(w->dctx);
        if (ZSTD_isError(result))
          goto error_clib;
      }
    }		/* decompress */

    /* read next input */
    in->size = in->allocated;
    rv = ctx->fn_read(ctx->arg_read, in);
    if (rv != 0) {
      result = mt_error(rv);
      goto error;
    }

    if (in->size == 0)
      goto okay;
    ctx->insize += in->size;

    zIn.size = in->size;
    zIn.pos = 0;
  }			/* read */

  error_clib:
  zstdmt_errcode = result;
  result = ZSTDMT_ERROR(compression_library);
  /* fall through */
  error:
  /* return with error */
  free(out->buf);
  free(in->buf);
  return result;
  okay:
  /* no error */
  free(out->buf);
  free(in->buf);
  return 0;
}

#define TYPE_UNKNOWN       0
#define TYPE_SINGLE_THREAD 1
#define TYPE_MULTI_THREAD  2

size_t ZSTDMT_decompressDCtx(ZSTDMT_DCtx * ctx, ZSTDMT_RdWr_t * rdwr)
{
  unsigned char buf[16];
  ZSTDMT_Buffer In;
  ZSTDMT_Buffer *in = &In;
  cwork_t *w;
  int t, rv, type = TYPE_UNKNOWN;
  void *retval_of_thread = 0;

  if (!ctx)
    return ZSTDMT_ERROR(compressionParameter_unsupported);

  /* init reading and writing functions */
  ctx->fn_read = rdwr->fn_read;
  ctx->fn_write = rdwr->fn_write;
  ctx->arg_read = rdwr->arg_read;
  ctx->arg_write = rdwr->arg_write;

  /**
   * possible valid magic's for us, we need 16 bytes, for checking
   *
   * 1) ZSTDMT_MAGIC @0 -> ST Stream
   * 2) ZSTDMT_MAGIC @0 + MAGIC_SKIPPABLE @9 -> MT Stream else ST
   * 3) MAGIC_SKIPPABLE @0 + ZSTDMT_MAGIC @12 -> MT Stream
   * 4) all other: not valid!
   */

  /* check for ZSTDMT_MAGIC_SKIPPABLE */
  in->buf = buf;
  in->size = 16;
  in->allocated = -1;
  rv = ctx->fn_read(ctx->arg_read, in);
  if (rv != 0)
    return mt_error(rv);

  /* must be single threaded standard zstd, when smaller 16 bytes */
  if (in->size < 16) {
    if (!IsZstd_Magic(buf))
      return ZSTDMT_ERROR(data_error);
    dprintf("single thread style, current pos=%zu\n", in->size);
    type = TYPE_SINGLE_THREAD;
    if (in->size == 9) {
      /* create empty file */
      ctx->threads = 0;
      ctx->cwork = 0;
      return 0;
    }
  } else {
    if (IsZstd_Skippable(buf) && IsZstd_Magic(buf + 12)) {
      /* pzstd */
      dprintf("pzstd style\n");
      type = TYPE_MULTI_THREAD;
    } else if (IsZstd_Magic(buf) && IsZstd_Skippable(buf + 9)) {
      /* zstdmt */
      dprintf("zstdmt style\n");
      type = TYPE_MULTI_THREAD;
      /* set buffer to the */
    } else if (IsZstd_Magic(buf)) {
      /* some std zstd stream */
      dprintf("single thread style, current pos=%zu\n",
              in->size);
      type = TYPE_SINGLE_THREAD;
    } else {
      /* invalid */
      dprintf("not valid\n");
      return ZSTDMT_ERROR(data_error);
    }
  }

  /* use single thread extraction, when only one thread is there */
  if (ctx->threadswanted == 1)
    type = TYPE_SINGLE_THREAD;

  /* single threaded, but with known sizes */
  if (type == TYPE_SINGLE_THREAD) {
    ctx->threads = 1;
    ctx->cwork = (cwork_t *) malloc(sizeof(cwork_t));
    if (!ctx->cwork)
      return ZSTDMT_ERROR(memory_allocation);
    w = &ctx->cwork[0];
    w->in.buf = in->buf;
    w->in.size = in->size;
    w->in.allocated = 0;
    w->ctx = ctx;
    w->dctx = ZSTD_createDStream();
    if (!w->dctx)
      return ZSTDMT_ERROR(memory_allocation);

    /* test, if pt_decompress is better... */
    return st_decompress(ctx);
  }

  /* setup thread work */
  ctx->threads = ctx->threadswanted;
  ctx->cwork = (cwork_t *) malloc(sizeof(cwork_t) * ctx->threads);
  if (!ctx->cwork)
    return ZSTDMT_ERROR(memory_allocation);

  for (t = 0; t < ctx->threads; t++) {
    w = &ctx->cwork[t];
    /* one of the threads must reuse the first bytes */
    w->in.buf = in->buf;
    w->in.size = in->size;
    w->in.allocated = 0;
    w->ctx = ctx;
    w->dctx = ZSTD_createDStream();
    if (!w->dctx)
      return ZSTDMT_ERROR(memory_allocation);
  }

  /* real multi threaded, init pthread's */
  pthread_mutex_init(&ctx->read_mutex, NULL);
  pthread_mutex_init(&ctx->write_mutex, NULL);
  pthread_mutex_init(&ctx->error_mutex, NULL);

  INIT_LIST_HEAD(&ctx->writelist_free);
  INIT_LIST_HEAD(&ctx->writelist_busy);
  INIT_LIST_HEAD(&ctx->writelist_done);

  /* multi threaded */
  for (t = 0; t < ctx->threads; t++) {
    cwork_t *wt = &ctx->cwork[t];
    pthread_create(&wt->pthread, NULL, pt_decompress, wt);
  }

  /* wait for all workers */
  for (t = 0; t < ctx->threads; t++) {
    cwork_t *wt = &ctx->cwork[t];
    void *p = 0;
    pthread_join(wt->pthread, &p);
    if (p)
      retval_of_thread = p;
  }

  /* clean up pthread stuff */
  pthread_mutex_destroy(&ctx->read_mutex);
  pthread_mutex_destroy(&ctx->write_mutex);
  pthread_mutex_destroy(&ctx->error_mutex);

  /* clean up the buffers */
  while (!list_empty(&ctx->writelist_free)) {
    struct writelist *wl;
    struct list_head *entry;
    entry = list_first(&ctx->writelist_free);
    wl = list_entry(entry, struct writelist, node);
    free(wl->out.buf);
    list_del(&wl->node);
    free(wl);
  }

  return (size_t) retval_of_thread;
}

/* returns current uncompressed data size */
size_t ZSTDMT_GetInsizeDCtx(ZSTDMT_DCtx * ctx)
{
  if (!ctx)
    return 0;

  return ctx->insize;
}

/* returns the current compressed data size */
size_t ZSTDMT_GetOutsizeDCtx(ZSTDMT_DCtx * ctx)
{
  if (!ctx)
    return 0;

  return ctx->outsize;
}

/* returns the current compressed frames */
size_t ZSTDMT_GetFramesDCtx(ZSTDMT_DCtx * ctx)
{
  if (!ctx)
    return 0;

  return ctx->curframe;
}

void ZSTDMT_freeDCtx(ZSTDMT_DCtx * ctx)
{
  int t;

  if (!ctx)
    return;

  for (t = 0; t < ctx->threads; t++) {
    cwork_t *w = &ctx->cwork[t];
    ZSTD_freeDStream(w->dctx);
  }

  if (ctx->cwork)
    free(ctx->cwork);

  free(ctx);
  ctx = 0;

  return;
}
