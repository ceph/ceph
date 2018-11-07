
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

#include <stdio.h>
#include <stdlib.h>

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"

#include "memmt.h"
#include "threading.h"
#include "list.h"
#include "zstd-mt.h"

/**
 * multi threaded zstd compression
 *
 * - each thread works on his own
 * - needs a callback for reading / writing
 * - each worker does this:
 *   1) get read mutex and read some input
 *   2) release read mutex and do compression
 *   3) get write mutex and write result
 *   4) begin with step 1 again, until no input
 */

/* worker for compression */
typedef struct {
  ZSTDMT_CCtx *ctx;
  pthread_t pthread;
} cwork_t;

struct writelist;
struct writelist {
  size_t frame;
  ZSTDMT_Buffer out;
  struct list_head node;
};

struct ZSTDMT_CCtx_s {

  /* level: 1..ZSTDMT_LEVEL_MAX */
  int level;

  /* threads: 1..ZSTDMT_THREAD_MAX */
  int threads;

  /* buffersize for reading input */
  int inputsize;

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
  size_t zstdmt_errcode;

  /* lists for writing queue */
  struct list_head writelist_free;
  struct list_head writelist_busy;
  struct list_head writelist_done;
};

/* **************************************
 * Compression
 ****************************************/

ZSTDMT_CCtx *ZSTDMT_createCCtx(int threads, int level, int inputsize)
{
  ZSTDMT_CCtx *ctx;
  int t;

  /* allocate ctx */
  ctx = (ZSTDMT_CCtx *) malloc(sizeof(ZSTDMT_CCtx));
  if (!ctx)
    return 0;

  /* check threads value */
  if (threads < 1 || threads > ZSTDMT_THREAD_MAX)
    goto err_ctx;

  /* check level */
  if (level < ZSTDMT_LEVEL_MIN || level > ZSTDMT_LEVEL_MAX)
    goto err_ctx;




  /* calculate chunksize for one thread */
  if (inputsize)
    ctx->inputsize = inputsize;
  else {
    const int windowLog[] = {
            19, 19, 20, 20, 20, /*  1 -  5 */
            21, 21, 21, 21, 21, /*  6 - 10 */
            22, 22, 22, 22, 22, /* 11 - 15 */
            23, 23, 23, 23, 25, /* 16 - 20 */
            26, 27
    };
    ctx->inputsize = 1 << (windowLog[level - 1] + 1);
  }

  /* setup ctx */
  ctx->level = level;
  ctx->threads = threads;

  pthread_mutex_init(&ctx->read_mutex, NULL);
  pthread_mutex_init(&ctx->write_mutex, NULL);
  pthread_mutex_init(&ctx->error_mutex, NULL);

  INIT_LIST_HEAD(&ctx->writelist_free);
  INIT_LIST_HEAD(&ctx->writelist_busy);
  INIT_LIST_HEAD(&ctx->writelist_done);

  ctx->cwork = (cwork_t *) malloc(sizeof(cwork_t) * threads);
  if (!ctx->cwork)
    goto err_ctx;

  for (t = 0; t < ctx->threads; t++) {
    cwork_t *w = &ctx->cwork[t];
    w->ctx = ctx;
  }

  return ctx;

  err_ctx:
  free(ctx);
  return 0;
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

  return ZSTDMT_ERROR(read_fail);
}

/**
 * pt_write - queue for compressed output
 */
static size_t pt_write(ZSTDMT_CCtx * ctx, struct writelist *wl)
{
  struct list_head *entry;
  int rv;

  /* move the entry to the done list */
  list_move(&wl->node, &ctx->writelist_done);

  /* the entry isn't the currently needed, return...  */
  if (wl->frame != ctx->curframe)
    return 0;

  again:
  /* check, what can be written ... */
  list_for_each(entry, &ctx->writelist_done) {
    wl = list_entry(entry, struct writelist, node);
    if (wl->frame == ctx->curframe) {
      rv = ctx->fn_write(ctx->arg_write, &wl->out);
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

/* parallel compression worker */
static void *pt_compress(void *arg)
{
  cwork_t *w = (cwork_t *) arg;
  ZSTDMT_CCtx *ctx = w->ctx;
  struct writelist *wl;
  size_t result;
  ZSTDMT_Buffer in;

  /* inbuf is constant */
  in.size = ctx->inputsize;
  in.buf = malloc(in.size);
  if (!in.buf)
    return (void *)ZSTDMT_ERROR(memory_allocation);

  for (;;) {
    struct list_head *entry;
    ZSTDMT_Buffer *out;
    int rv;

    /* allocate space for new output */
    pthread_mutex_lock(&ctx->write_mutex);
    if (!list_empty(&ctx->writelist_free)) {
      /* take unused entry */
      entry = list_first(&ctx->writelist_free);
      wl = list_entry(entry, struct writelist, node);
      wl->out.size = ZSTD_compressBound(ctx->inputsize) + 12;
      list_move(entry, &ctx->writelist_busy);
    } else {
      /* allocate new one */
      wl = (struct writelist *)
              malloc(sizeof(struct writelist));
      if (!wl) {
        pthread_mutex_unlock(&ctx->write_mutex);
        free(in.buf);
        return (void *)ZSTDMT_ERROR(memory_allocation);
      }
      wl->out.size = ZSTD_compressBound(ctx->inputsize) + 12;;
      wl->out.buf = malloc(wl->out.size);
      if (!wl->out.buf) {
        pthread_mutex_unlock(&ctx->write_mutex);
        free(in.buf);
        return (void *)ZSTDMT_ERROR(memory_allocation);
      }
      list_add(&wl->node, &ctx->writelist_busy);
    }
    pthread_mutex_unlock(&ctx->write_mutex);
    out = &wl->out;

    /* read new input */
    pthread_mutex_lock(&ctx->read_mutex);
    in.size = ctx->inputsize;
    rv = ctx->fn_read(ctx->arg_read, &in);

    if (rv != 0) {
      pthread_mutex_unlock(&ctx->read_mutex);
      result = mt_error(rv);
      goto error;
    }

    /* eof */
    if (in.size == 0 && ctx->frames > 0) {
      free(in.buf);
      pthread_mutex_unlock(&ctx->read_mutex);

      pthread_mutex_lock(&ctx->write_mutex);
      list_move(&wl->node, &ctx->writelist_free);
      pthread_mutex_unlock(&ctx->write_mutex);

      goto okay;
    }
    ctx->insize += in.size;
    wl->frame = ctx->frames++;
    pthread_mutex_unlock(&ctx->read_mutex);

    /* compress whole frame */
    {
      unsigned char *outbuf = out->buf;
      result =
              ZSTD_compress(outbuf + 12, out->size - 12, in.buf,
                            in.size, ctx->level);
      if (ZSTD_isError(result)) {
        zstdmt_errcode = result;
        result = ZSTDMT_ERROR(compression_library);
        goto error;
      }
    }

    /* write skippable frame */
    {
      unsigned char *outbuf = out->buf;

      MEM_writeLE32(outbuf + 0, ZSTDMT_MAGIC_SKIPPABLE);
      MEM_writeLE32(outbuf + 4, 4);
      MEM_writeLE32(outbuf + 8, (U32) result);
      out->size = result + 12;
    }

    /* write result */
    pthread_mutex_lock(&ctx->write_mutex);
    result = pt_write(ctx, wl);
    pthread_mutex_unlock(&ctx->write_mutex);
    if (ZSTDMT_isError(result))
      goto error;
  }

  okay:
  return 0;
  error:
  pthread_mutex_lock(&ctx->write_mutex);
  list_move(&wl->node, &ctx->writelist_free);
  pthread_mutex_unlock(&ctx->write_mutex);
  return (void *)result;
}

/* compress data, until input ends */
size_t ZSTDMT_compressCCtx(ZSTDMT_CCtx * ctx, ZSTDMT_RdWr_t * rdwr)
{
  int t;
  void *retval_of_thread = 0;

  if (!ctx)
    return ZSTDMT_ERROR(init_missing);

  /* setup reading and writing functions */
  ctx->fn_read = rdwr->fn_read;
  ctx->fn_write = rdwr->fn_write;
  ctx->arg_read = rdwr->arg_read;
  ctx->arg_write = rdwr->arg_write;

  /* init counter and error codes */
  ctx->insize = 0;
  ctx->outsize = 0;
  ctx->frames = 0;
  ctx->curframe = 0;
  ctx->zstdmt_errcode = 0;

  /* start all workers */
  for (t = 0; t < ctx->threads; t++) {
    cwork_t *w = &ctx->cwork[t];
    pthread_create(&w->pthread, NULL, pt_compress, w);
  }

  /* wait for all workers */
  for (t = 0; t < ctx->threads; t++) {
    cwork_t *w = &ctx->cwork[t];
    void *p = 0;
    pthread_join(w->pthread, &p);
    if (p)
      retval_of_thread = p;
  }

  /* clean up the free list */
  while (!list_empty(&ctx->writelist_free)) {
    struct writelist *wl;
    struct list_head *entry;
    entry = list_first(&ctx->writelist_free);
    wl = list_entry(entry, struct writelist, node);
    free(wl->out.buf);
    list_del(&wl->node);
    free(wl);
  }

  /* on error, these two lists may have some entries */
  if (retval_of_thread) {
    struct writelist *wl;
    struct list_head *entry;

    while (!list_empty(&ctx->writelist_busy)) {
      entry = list_first(&ctx->writelist_busy);
      wl = list_entry(entry, struct writelist, node);
      free(wl->out.buf);
      list_del(&wl->node);
      free(wl);
    }

    while (!list_empty(&ctx->writelist_done)) {
      entry = list_first(&ctx->writelist_done);
      wl = list_entry(entry, struct writelist, node);
      free(wl->out.buf);
      list_del(&wl->node);
      free(wl);
    }
  }

  return (size_t) retval_of_thread;
}

/* returns current uncompressed data size */
size_t ZSTDMT_GetInsizeCCtx(ZSTDMT_CCtx * ctx)
{
  if (!ctx)
    return ZSTDMT_ERROR(init_missing);

  /* no mutex needed here */
  return ctx->insize;
}

/* returns the current compressed data size */
size_t ZSTDMT_GetOutsizeCCtx(ZSTDMT_CCtx * ctx)
{
  if (!ctx)
    return ZSTDMT_ERROR(init_missing);

  /* no mutex needed here */
  return ctx->outsize;
}

/* returns the current compressed data frame count */
size_t ZSTDMT_GetFramesCCtx(ZSTDMT_CCtx * ctx)
{
  if (!ctx)
    return ZSTDMT_ERROR(init_missing);

  /* no mutex needed here */
  return ctx->curframe;
}

/* free all allocated buffers and structures */
void ZSTDMT_freeCCtx(ZSTDMT_CCtx * ctx)
{
  if (!ctx)
    return;

  free(ctx->cwork);
  free(ctx);
  ctx = 0;

  return;
}
