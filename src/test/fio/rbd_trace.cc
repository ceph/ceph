/*
 * rbd engine
 *
 * IO engine using Ceph's librbd to test RADOS Block Devices.
 *
 */

#include <rbd/librbd.h>
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "common/EventTrace.h"
#include "include/msgr.h"
#include <fio.h>
#include <optgroup.h>
#ifdef CONFIG_RBD_BLKIN
#include <zipkin_c.h>
#endif

#ifdef CONFIG_RBD_POLL
/* add for poll */
#include <poll.h>
#include <sys/eventfd.h>
#endif
#define log_info printf
#define log_warn printf
#define log_err printf


extern void init_context(void);

namespace {
struct fio_rbd_iou {
  struct io_u *io_u;
  rbd_completion_t completion;
  int io_seen;
  int io_complete;
#ifdef CONFIG_RBD_BLKIN
  struct blkin_trace_info info;
#endif
};

struct rbd_data {
  rados_t cluster;
  rados_ioctx_t io_ctx;
  rbd_image_t image;
  struct io_u **aio_events;
  struct io_u **sort_events;
  int fd; /* add for poll */
};

struct rbd_options {
  void *pad;
  char *cluster_name;
  char *rbd_name;
  char *pool_name;
  char *client_name;
  int busy_poll;
};

int _fio_setup_rbd_data(struct thread_data *td,
             struct rbd_data **rbd_data_ptr)
{
  struct rbd_data *rbd;

  if (td->io_ops_data)
    return 0;

  rbd = (struct rbd_data *) calloc(1, sizeof(struct rbd_data));
  if (!rbd)
    goto failed;

  /* add for poll, init fd: -1 */
  rbd->fd = -1;

  rbd->aio_events = (struct io_u **) calloc(td->o.iodepth, sizeof(struct io_u *));
  if (!rbd->aio_events)
    goto failed;

  rbd->sort_events = (struct io_u **) calloc(td->o.iodepth, sizeof(struct io_u *));
  if (!rbd->sort_events)
    goto failed;

  *rbd_data_ptr = rbd;
  return 0;

failed:
  if (rbd) {
    if (rbd->aio_events) 
      free(rbd->aio_events);
    if (rbd->sort_events)
      free(rbd->sort_events);
    free(rbd);
  }
  return 1;

}

#ifdef CONFIG_RBD_POLL
bool _fio_rbd_setup_poll(struct rbd_data *rbd)
{
  int r;

  /* add for rbd poll */
  rbd->fd = eventfd(0, EFD_NONBLOCK);
  if (rbd->fd < 0) {
    log_err("eventfd failed.\n");
    return false;
  }

  r = rbd_set_image_notification(rbd->image, rbd->fd, EVENT_TYPE_EVENTFD);
  if (r < 0) {
    log_err("rbd_set_image_notification failed.\n");
    close(rbd->fd);
    rbd->fd = -1;
    return false;
  }

  return true;
}
#else
bool _fio_rbd_setup_poll(struct rbd_data *rbd)
{
  return true;
}
#endif

int _fio_rbd_connect(struct thread_data *td)
{
  static bool init = 0;
  struct rbd_data *rbd = (struct rbd_data *) td->io_ops_data;
  struct rbd_options *o = ( struct rbd_options *) td->eo;
  int r;
  static boost::intrusive_ptr<CephContext> cct;

  if (!init) {
    std::vector < const char* > args;
    args.push_back("--cluster");
    args.push_back(o->cluster_name ? o->cluster_name : "ceph");
    env_to_vec(args);
    cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                      CODE_ENVIRONMENT_UTILITY, 0);
    common_init_finish(g_ceph_context);
    init = 1;
  }

  if (o->cluster_name) {
    char *client_name = NULL; 

    /*
     * If we specify cluser name, the rados_create2
     * will not assume 'client.'. name is considered
     * as a full type.id namestr
     */
    if (o->client_name) {
      if (!index(o->client_name, '.')) {
        client_name = (char *) calloc(1, strlen("client.") +
                strlen(o->client_name) + 1);
        strcat(client_name, "client.");
        strcat(client_name, o->client_name);
      } else {
        client_name = o->client_name;
      }
    }

    r = rados_create2(&rbd->cluster, o->cluster_name,
         client_name, 0);

    if (client_name && !index(o->client_name, '.'))
      free(client_name);
  } else
    r = rados_create(&rbd->cluster, o->client_name);
  
  if (r < 0) {
    log_err("rados_create failed.\n");
    goto failed_early;
  }

  r = rados_conf_read_file(rbd->cluster, NULL);
  if (r < 0) {
    log_err("rados_conf_read_file failed.\n");
    goto failed_early;
  }

  r = rados_connect(rbd->cluster);
  if (r < 0) {
    log_err("rados_connect failed.\n");
    goto failed_shutdown;
  }

  r = rados_ioctx_create(rbd->cluster, o->pool_name, &rbd->io_ctx);
  if (r < 0) {
    log_err("rados_ioctx_create failed.\n");
    goto failed_shutdown;
  }

  r = rbd_open(rbd->io_ctx, o->rbd_name, &rbd->image, NULL /*snap */ );
  if (r < 0) {
    log_err("rbd_open failed.\n");
    goto failed_open;
  }

  if (!_fio_rbd_setup_poll(rbd))
    goto failed_poll;

  return 0;

failed_poll:
  rbd_close(rbd->image);
  rbd->image = NULL;
failed_open:
  rados_ioctx_destroy(rbd->io_ctx);
  rbd->io_ctx = NULL;
failed_shutdown:
  rados_shutdown(rbd->cluster);
  rbd->cluster = NULL;
failed_early:
  return 1;
}

void _fio_rbd_disconnect(struct rbd_data *rbd)
{
  if (!rbd)
    return;

  /* close eventfd */
  if (rbd->fd != -1) {
    close(rbd->fd);
    rbd->fd = -1;
  }

  /* shutdown everything */
  if (rbd->image) {
    rbd_close(rbd->image);
    rbd->image = NULL;
  }

  if (rbd->io_ctx) {
    rados_ioctx_destroy(rbd->io_ctx);
    rbd->io_ctx = NULL;
  }

  if (rbd->cluster) {
    rados_shutdown(rbd->cluster);
    rbd->cluster = NULL;
  }
}

void _fio_rbd_finish_aiocb(rbd_completion_t comp, void *data)
{
  struct fio_rbd_iou *fri = ( struct fio_rbd_iou *) data;
  struct io_u *io_u = (struct io_u *) fri->io_u;
  ssize_t ret;

  /*
   * Looks like return value is 0 for success, or < 0 for
   * a specific error. So we have to assume that it can't do
   * partial completions.
   */
  ret = rbd_aio_get_return_value(fri->completion);
  if (ret < 0) {
    io_u->error = ret;
    io_u->resid = io_u->xfer_buflen;
  } else
    io_u->error = 0;

  fri->io_complete = 1;
}

struct io_u *fio_rbd_event(struct thread_data *td, int event)
{
  struct rbd_data *rbd = (struct rbd_data *) td->io_ops_data;

  return rbd->aio_events[event];
}

inline int fri_check_complete(struct rbd_data *rbd, struct io_u *io_u,
             unsigned int *events)
{
  struct fio_rbd_iou *fri = (struct fio_rbd_iou *) io_u->engine_data;

  if (fri->io_complete) {
    fri->io_seen = 1;
    rbd->aio_events[*events] = io_u;
    (*events)++;

    rbd_aio_release(fri->completion);
    return 1;
  }

  return 0;
}

inline int rbd_io_u_seen(struct io_u *io_u)
{
  struct fio_rbd_iou *fri = (struct fio_rbd_iou *) io_u->engine_data;

  return fri->io_seen;
}

void rbd_io_u_wait_complete(struct io_u *io_u)
{
  struct fio_rbd_iou *fri = (struct fio_rbd_iou *) io_u->engine_data;

  rbd_aio_wait_for_complete(fri->completion);
}

int rbd_io_u_cmp(const void *p1, const void *p2)
{
  const struct io_u **a = (const struct io_u **) p1;
  const struct io_u **b = (const struct io_u **) p2;
  uint64_t at, bt;

  at = utime_since_now(&(*a)->start_time);
  bt = utime_since_now(&(*b)->start_time);

  if (at < bt)
    return -1;
  else if (at == bt)
    return 0;
  else
    return 1;
}

int rbd_iter_events(struct thread_data *td, unsigned int *events,
         unsigned int min_evts, int wait)
{
  struct rbd_data *rbd = (struct rbd_data *) td->io_ops_data;
  unsigned int this_events = 0;
  struct io_u *io_u;
  int i;
        int sidx = 0;

#ifdef CONFIG_RBD_POLL
  int ret = 0;
  int event_num = 0;
  struct fio_rbd_iou *fri = NULL;
  rbd_completion_t comps[min_evts];

  struct pollfd pfd;
  pfd.fd = rbd->fd;
  pfd.events = POLLIN;

  ret = poll(&pfd, 1, -1);
  if (ret <= 0)
    return 0;

  assert(pfd.revents & POLLIN);

  event_num = rbd_poll_io_events(rbd->image, comps, min_evts);

  for (i = 0; i < event_num; i++) {
    fri = rbd_aio_get_arg(comps[i]);
    io_u = fri->io_u;
#else
  io_u_qiter(&td->io_u_all, io_u, i) {
#endif
    if (!(io_u->flags & IO_U_F_FLIGHT))
      continue;
    if (rbd_io_u_seen(io_u))
      continue;

    if (fri_check_complete(rbd, io_u, events))
      this_events++;
    else if (wait)
      rbd->sort_events[sidx++] = io_u;
  }

  if (!wait || !sidx)
    return this_events;

  /*
   * Sort events, oldest issue first, then wait on as many as we
   * need in order of age. If we have enough events, stop waiting,
   * and just check if any of the older ones are done.
   */
  if (sidx > 1)
    qsort(rbd->sort_events, sidx, sizeof(struct io_u *), rbd_io_u_cmp);

  for (i = 0; i < sidx; i++) {
    io_u = rbd->sort_events[i];

    if (fri_check_complete(rbd, io_u, events)) {
      this_events++;
      continue;
    }

    /*
     * Stop waiting when we have enough, but continue checking
     * all pending IOs if they are complete.
     */
    if (*events >= min_evts)
      continue;

    rbd_io_u_wait_complete(io_u);

    if (fri_check_complete(rbd, io_u, events))
      this_events++;
  }

  return this_events;
}

int fio_rbd_getevents(struct thread_data *td, unsigned int min,
           unsigned int max, const struct timespec *t)
{
  unsigned int this_events, events = 0;
  struct rbd_options *o = (struct rbd_options *) td->eo;
  int wait = 0;

  do {
    this_events = rbd_iter_events(td, &events, min, wait);

    if (events >= min)
      break;
    if (this_events)
      continue;

    if (!o->busy_poll)
      wait = 1;
    else
      nop;
  } while (1);

  return events;
}

int fio_rbd_queue(struct thread_data *td, struct io_u *io_u)
{
  struct rbd_data *rbd = (struct rbd_data *) td->io_ops_data;
  struct fio_rbd_iou *fri = (struct fio_rbd_iou *) io_u->engine_data;
  int r = -1;

  fio_ro_check(td, io_u);

  fri->io_seen = 0;
  fri->io_complete = 0;

  r = rbd_aio_create_completion(fri, _fio_rbd_finish_aiocb,
            &fri->completion);
  if (r < 0) {
    log_err("rbd_aio_create_completion failed.\n");
    goto failed;
  }

  if (io_u->ddir == DDIR_WRITE) {
#ifdef CONFIG_RBD_BLKIN
    blkin_init_trace_info(&fri->info);
    r = rbd_aio_write_traced(rbd->image, io_u->offset, io_u->xfer_buflen,
           io_u->xfer_buf, fri->completion, &fri->info);
#else
    r = rbd_aio_write(rbd->image, io_u->offset, io_u->xfer_buflen,
           (const char *) io_u->xfer_buf, fri->completion);
#endif
    if (r < 0) {
      log_err("rbd_aio_write failed.\n");
      goto failed_comp;
    }

  } else if (io_u->ddir == DDIR_READ) {
#ifdef CONFIG_RBD_BLKIN
    blkin_init_trace_info(&fri->info);
    r = rbd_aio_read_traced(rbd->image, io_u->offset, io_u->xfer_buflen,
          io_u->xfer_buf, fri->completion, &fri->info);
#else
    r = rbd_aio_read(rbd->image, io_u->offset, io_u->xfer_buflen,
          (char *) io_u->xfer_buf, fri->completion);
#endif

    if (r < 0) {
      log_err("rbd_aio_read failed.\n");
      goto failed_comp;
    }
  } else if (io_u->ddir == DDIR_TRIM) {
    r = rbd_aio_discard(rbd->image, io_u->offset,
          io_u->xfer_buflen, fri->completion);
    if (r < 0) {
      log_err("rbd_aio_discard failed.\n");
      goto failed_comp;
    }
  } else if (io_u->ddir == DDIR_SYNC) {
    r = rbd_aio_flush(rbd->image, fri->completion);
    if (r < 0) {
      log_err("rbd_flush failed.\n");
      goto failed_comp;
    }
  } else {
    dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
           io_u->ddir);
    goto failed_comp;
  }

  return FIO_Q_QUEUED;
failed_comp:
  rbd_aio_release(fri->completion);
failed:
  io_u->error = r;
  td_verror(td, io_u->error, "xfer");
  return FIO_Q_COMPLETED;
}

int fio_rbd_init(struct thread_data *td)
{
  int r;

  r = _fio_rbd_connect(td);
  if (r) {
    log_err("fio_rbd_connect failed, return code: %d .\n", r);
    goto failed;
  }

  return 0;

failed:
  return 1;
}

void fio_rbd_cleanup(struct thread_data *td)
{
  struct rbd_data *rbd = (struct rbd_data *) td->io_ops_data;

  if (rbd) {
    _fio_rbd_disconnect(rbd);
    free(rbd->aio_events);
    free(rbd->sort_events);
    free(rbd);
  }
}

int fio_rbd_setup(struct thread_data *td)
{
  rbd_image_info_t info;
  struct fio_file *f;
  struct rbd_data *rbd = NULL;
  int major, minor, extra;
  int r;

  /* log version of librbd. No cluster connection required. */
  rbd_version(&major, &minor, &extra);
  log_info("rbd engine: RBD version: %d.%d.%d\n", major, minor, extra);

  /* allocate engine specific structure to deal with librbd. */
  r = _fio_setup_rbd_data(td, &rbd);
  if (r) {
    log_err("fio_setup_rbd_data failed.\n");
    goto cleanup;
  }
  td->io_ops_data = rbd;

  /* librbd does not allow us to run first in the main thread and later
   * in a fork child. It needs to be the same process context all the
   * time. 
   */
  td->o.use_thread = 1;

  /* connect in the main thread to determine to determine
   * the size of the given RADOS block device. And disconnect
   * later on.
   */
  r = _fio_rbd_connect(td);
  if (r) {
    log_err("fio_rbd_connect failed.\n");
    goto cleanup;
  }

  /* get size of the RADOS block device */
  r = rbd_stat(rbd->image, &info, sizeof(info));
  if (r < 0) {
    log_err("rbd_status failed.\n");
    goto disconnect;
  } else if (info.size == 0) {
    log_err("image size should be larger than zero.\n");
    r = -EINVAL;
    goto disconnect;
  }

  dprint(FD_IO, "rbd-engine: image size: %lu\n", info.size);

  /* taken from "net" engine. Pretend we deal with files,
   * even if we do not have any ideas about files.
   * The size of the RBD is set instead of a artificial file.
   */
  if (!td->files_index) {
    add_file(td, td->o.filename ? : "rbd", 0, 0);
    td->o.nr_files = td->o.nr_files ? : 1;
    td->o.open_files++;
  }
  f = td->files[0];
  f->real_file_size = info.size;

  /* disconnect, then we were only connected to determine
   * the size of the RBD.
   */
  _fio_rbd_disconnect(rbd);
  return 0;

disconnect:
  _fio_rbd_disconnect(rbd);
cleanup:
  fio_rbd_cleanup(td);
  return r;
}

int fio_rbd_open(struct thread_data *td, struct fio_file *f)
{
  return 0;
}

int fio_rbd_invalidate(struct thread_data *td, struct fio_file *f)
{
#if defined(CONFIG_RBD_INVAL)
  struct rbd_data *rbd = td->io_ops_data;

  return rbd_invalidate_cache(rbd->image);
#else
  return 0;
#endif
}

void fio_rbd_io_u_free(struct thread_data *td, struct io_u *io_u)
{
  struct fio_rbd_iou *fri = (struct fio_rbd_iou *) io_u->engine_data;

  if (fri) {
    io_u->engine_data = NULL;
    free(fri);
  }
}

int fio_rbd_io_u_init(struct thread_data *td, struct io_u *io_u)
{
  struct fio_rbd_iou *fri;

  fri = (struct fio_rbd_iou *) calloc(1, sizeof(*fri));
  fri->io_u = io_u;
  io_u->engine_data = fri;
  return 0;
}

char *init_str(const char* str)
{
  char *s = (char *) calloc(strlen(str)+1, sizeof(char));
  strcpy(s, str); 
  return s;
}

struct ioengine_ops *fio_init fio_init_rbd(void)
{
  struct fio_option * options = (struct fio_option *) calloc(6, sizeof(struct fio_option));
  options[0].name           = init_str("clustername");
  options[0].lname          = init_str("ceph cluster name");
  options[0].type           = FIO_OPT_STR_STORE;
  options[0].help           = init_str("Cluster name for ceph");
  options[0].off1           = offsetof(struct rbd_options, cluster_name);
  options[0].category       = FIO_OPT_C_ENGINE;
  options[0].group          = FIO_OPT_G_RBD;

  options[1].name           = init_str("rbdname");
  options[1].lname          = init_str("rbd engine rbdname");
  options[1].type           = FIO_OPT_STR_STORE;
  options[1].help           = init_str("RBD name for RBD engine");
  options[1].off1           = offsetof(struct rbd_options, rbd_name);
  options[1].category       = FIO_OPT_C_ENGINE;
  options[1].group          = FIO_OPT_G_RBD;

  options[2].name           = init_str("pool");
  options[2].lname          = init_str("rbd engine pool");
  options[2].type           = FIO_OPT_STR_STORE;
  options[2].help           = init_str("Name of the pool hosting the RBD for the RBD engine");
  options[2].off1           = offsetof(struct rbd_options, pool_name);
  options[2].category       = FIO_OPT_C_ENGINE;
  options[2].group          = FIO_OPT_G_RBD;

  options[3].name           = init_str("clientname");
  options[3].lname          = init_str("rbd engine clientname");
  options[3].type           = FIO_OPT_STR_STORE;
  options[3].help           = init_str("Name of the ceph client to access the RBD for the RBD engine");
  options[3].off1           = offsetof(struct rbd_options, client_name);
  options[3].category       = FIO_OPT_C_ENGINE;
  options[3].group          = FIO_OPT_G_RBD;

  options[4].name           = init_str("busy_poll");
  options[4].lname          = init_str("Busy poll");
  options[4].type           = FIO_OPT_BOOL;
  options[4].help           = init_str("Busy poll for completions instead of sleeping");
  options[4].off1           = offsetof(struct rbd_options, busy_poll);
  options[4].def            = init_str("0");
  options[4].category       = FIO_OPT_C_ENGINE;
  options[4].group          = FIO_OPT_G_RBD;

  options[5].name           = NULL;

  struct ioengine_ops * ioengine = (struct ioengine_ops *) malloc(sizeof(struct ioengine_ops));
  ioengine->name                    = init_str("rbd_trace");
  ioengine->version                = FIO_IOOPS_VERSION;
  ioengine->setup                  = fio_rbd_setup;
  ioengine->init                   = fio_rbd_init;
  ioengine->queue                  = fio_rbd_queue;
  ioengine->getevents              = fio_rbd_getevents;
  ioengine->event                  = fio_rbd_event;
  ioengine->cleanup                = fio_rbd_cleanup;
  ioengine->open_file              = fio_rbd_open;
  ioengine->invalidate             = fio_rbd_invalidate;
  ioengine->options                = options;
  ioengine->io_u_init              = fio_rbd_io_u_init;
  ioengine->io_u_free              = fio_rbd_io_u_free;
  ioengine->option_struct_size     = sizeof(struct rbd_options);

  return ioengine;
}
}
extern "C" {
  void get_ioengine(struct ioengine_ops **ioengine_ptr) 
  {
    *ioengine_ptr = fio_init_rbd();
  }
}
