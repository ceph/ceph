// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdint.h>
#include <tuple>
#include <vector>
#include <functional>
#include <iostream>

#include <semaphore.h> // XXX kill this?

#include "fmt/include/fmt/format.h"

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
//#include "rgw/rgw_file.h"
//#include "rgw/rgw_lib_frontend.h" // direct requests

/* naughty fio.h leaks min and max as C macros--include it last */
#include <fio.h>
#include <optgroup.h>
#undef min
#undef max

namespace {

  struct librgw_iou {
    struct io_u *io_u;
    int io_complete;
  };

  struct librgw_data {
    io_u** aio_events;
    librgw_t rgw_h;
    rgw_fs* fs;
    rgw_file_handle* bucket_fh;

    std::vector<rgw_file_handle*> fh_vec;

    librgw_data(thread_data* td)
      : rgw_h(nullptr), fs(nullptr), bucket_fh(nullptr)
      {
	auto size = td->o.iodepth * sizeof(io_u*);
	aio_events = static_cast<io_u**>(malloc(size));
	memset(aio_events, 0, size);
      }

    void save_handle(rgw_file_handle* fh) {
      fh_vec.push_back(fh);
    }

    void release_handles() {
      for (auto object_fh : fh_vec) {
	rgw_fh_rele(fs, object_fh, RGW_FH_RELE_FLAG_NONE);
      }
      fh_vec.clear();
    }

    ~librgw_data() {
      free(aio_events);
    }
  };

  struct opt_struct {
    struct thread_data *td;

    const char* config; /* can these be std::strings? */
    const char* cluster;
    const char* name; // instance?
    const char* init_args;
    const char* access_key;
    const char* secret_key;
    const char* userid;
    const char* bucket_name;

    uint32_t owner_uid = 867;
    uint32_t owner_gid = 5309;
  };

  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

/* borrowed from fio_ceph_objectstore */
  template <class F>
  fio_option make_option(F&& func)
  {
    // zero-initialize and set common defaults
    auto o = fio_option{};
    o.category = FIO_OPT_C_ENGINE;
    o.group    = FIO_OPT_G_INVALID;
    func(std::ref(o));
    return o;
  }

  static std::vector<fio_option> options = {
    make_option([] (fio_option& o) {
		  o.name   = "ceph_conf";
		  o.lname  = "ceph configuration file";
		  o.type   = FIO_OPT_STR_STORE;
		  o.help   = "Path to ceph.conf file";
		  o.off1   = offsetof(opt_struct, config);
		}),
    make_option([] (fio_option& o) {
		  o.name     = "ceph_name";
		  o.lname    = "ceph instance name";
		  o.type     = FIO_OPT_STR_STORE;
		  o.help     = "Name of this program instance";
		  o.off1     = offsetof(opt_struct, name);
		  o.category = FIO_OPT_C_ENGINE;
		  o.group    = FIO_OPT_G_INVALID;
		}),
    make_option([] (fio_option& o) {
		  o.name     = "ceph_cluster";
		  o.lname    = "ceph cluster name";
		  o.type     = FIO_OPT_STR_STORE;
		  o.help     = "Name of ceph cluster (default=ceph)";
		  o.off1     = offsetof(opt_struct, cluster);
		  o.category = FIO_OPT_C_ENGINE;
		  o.group    = FIO_OPT_G_INVALID;
		}),
    make_option([] (fio_option& o) {
		  o.name     = "ceph_init_args";
		  o.lname    = "ceph init args";
		  o.type     = FIO_OPT_STR_STORE;
		  o.help     = "Extra ceph arguments (e.g., -d --debug-rgw=16)";
		  o.off1     = offsetof(opt_struct, init_args);
		  o.category = FIO_OPT_C_ENGINE;
		  o.group    = FIO_OPT_G_INVALID;
		}),
    make_option([] (fio_option& o) {
		  o.name     = "access_key";
		  o.lname    = "AWS access key";
		  o.type     = FIO_OPT_STR_STORE;
		  o.help     = "AWS access key";
		  o.off1     = offsetof(opt_struct, access_key);
		  o.category = FIO_OPT_C_ENGINE;
		  o.group    = FIO_OPT_G_INVALID;
		}),
    make_option([] (fio_option& o) {
		  o.name     = "secret_key";
		  o.lname    = "AWS secret key";
		  o.type     = FIO_OPT_STR_STORE;
		  o.help     = "AWS secret key";
		  o.off1     = offsetof(opt_struct, secret_key);
		  o.category = FIO_OPT_C_ENGINE;
		  o.group    = FIO_OPT_G_INVALID;
		}),
    make_option([] (fio_option& o) {
		  o.name     = "userid";
		  o.lname    = "userid";
		  o.type     = FIO_OPT_STR_STORE;
		  o.help     = "userid corresponding to access key";
		  o.off1     = offsetof(opt_struct, userid);
		  o.category = FIO_OPT_C_ENGINE;
		  o.group    = FIO_OPT_G_INVALID;
		}),
    make_option([] (fio_option& o) {
		  o.name     = "bucket_name";
		  o.lname    = "S3 bucket";
		  o.type     = FIO_OPT_STR_STORE;
		  o.help     = "S3 bucket to operate on";
		  o.off1     = offsetof(opt_struct, bucket_name);
		  o.category = FIO_OPT_C_ENGINE;
		  o.group    = FIO_OPT_G_INVALID;
		}),
    {} // fio expects a 'null'-terminated list
  };

  struct save_args {
    int argc;
    char *argv[8];
    save_args() : argc(1)
      {
	argv[0] = strdup("librgw");
	for (int ix = 1; ix < 8; ++ix) {
	  argv[ix] = nullptr;
	}
      }

    void push_arg(const std::string sarg) {
      argv[argc++] = strdup(sarg.c_str());
    }

    ~save_args() {
      for (int ix = 0; ix < argc; ++ix) {
	argv[ix] = nullptr;
      }
    }
  } args;

/*
 * It looks like the setup function is called once, on module load.
 * It's not documented in the skeleton driver.
 */
  static int fio_librgw_setup(struct thread_data* td)
  {
    opt_struct& o = *(reinterpret_cast<opt_struct*>(td->eo));
    librgw_data* data = nullptr;
    int r = 0;

    dprint(FD_IO, "fio_librgw_setup\n");

    if (! td->io_ops_data) {
      data = new librgw_data(td);

      /* init args */
      std::string sopt;
      if (o.config) {
	sopt = fmt::format("--conf={}", o.config);
	args.push_arg(sopt);
      }
      std::cout << o.name << std::endl;
      if (o.name) {
	sopt = fmt::format("--name={}", o.name);
	args.push_arg(sopt);
      }
      if (o.cluster) {
	sopt = fmt::format("--cluster={}", o.cluster);
	args.push_arg(sopt);
      }
      if (o.init_args) {
	args.push_arg(std::string(o.init_args));
      }

      r = librgw_create(&data->rgw_h, args.argc, args.argv);
      if (!! r) {
	dprint(FD_IO, "librgw_create failed\n");
	return r;
      }

      r = rgw_mount2(data->rgw_h, o.userid, o.access_key, o.secret_key, "/",
		     &data->fs, RGW_MOUNT_FLAG_NONE);
      if (!! r) {
	dprint(FD_IO, "rgw_mount2 failed\n");
	return r;
      }

      /* go ahead and lookup the bucket as well */
      r = rgw_lookup(data->fs, data->fs->root_fh, o.bucket_name,
		     &data->bucket_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE);
      if (! data->bucket_fh) {
	dprint(FD_IO, "rgw_lookup on bucket %s failed, will create\n",
	       o.bucket_name);

	struct stat st;
	st.st_uid = o.owner_uid;
	st.st_gid = o.owner_gid;
	st.st_mode = 755;

	r = rgw_mkdir(data->fs, data->fs->root_fh, o.bucket_name,
		      &st, create_mask, &data->bucket_fh, RGW_MKDIR_FLAG_NONE);
	if (! data->bucket_fh) {
	  dprint(FD_IO, "rgw_mkdir for bucket %s failed\n", o.bucket_name);
	  return EINVAL;
	}
      }

      td->io_ops_data = data;
    }

    td->o.use_thread = 1;

    if (r != 0) {
      abort();
    }

    return r;
  }

/*
 * The init function is called once per thread/process, and should set up
 * any structures that this io engine requires to keep track of io. Not
 * required.
 */
  static int fio_librgw_init(struct thread_data *td)
  {
    dprint(FD_IO, "fio_librgw_init\n");
    return 0;
  }

/*
 * This is paired with the ->init() function and is called when a thread is
 * done doing io. Should tear down anything setup by the ->init() function.
 * Not required.
 *
 * N.b., the cohort driver made this idempotent by allocating data in
 * setup, clearing data here if present, and doing nothing in the
 * subsequent per-thread invocations.
 */
  static void fio_librgw_cleanup(struct thread_data *td)
  {
    dprint(FD_IO, "fio_librgw_cleanup\n");

    /* cleanup specific data */
    librgw_data* data = static_cast<librgw_data*>(td->io_ops_data);
    if (data) {

      /* release active handles */
      data->release_handles();

      if (data->bucket_fh) {
	rgw_fh_rele(data->fs, data->bucket_fh, 0 /* flags */);
      }
      rgw_umount(data->fs, RGW_UMOUNT_FLAG_NONE);
      librgw_shutdown(data->rgw_h);
      td->io_ops_data = nullptr;
      delete data;
    }
  }

/*
 * The ->prep() function is called for each io_u prior to being submitted
 * with ->queue(). This hook allows the io engine to perform any
 * preparatory actions on the io_u, before being submitted. Not required.
 */
  static int fio_librgw_prep(struct thread_data *td, struct io_u *io_u)
  {
    return 0;
  }

/*
 * The ->event() hook is called to match an event number with an io_u.
 * After the core has called ->getevents() and it has returned eg 3,
 * the ->event() hook must return the 3 events that have completed for
 * subsequent calls to ->event() with [0-2]. Required.
 */
  static struct io_u *fio_librgw_event(struct thread_data *td, int event)
  {
    return NULL;
  }

/*
 * The ->getevents() hook is used to reap completion events from an async
 * io engine. It returns the number of completed events since the last call,
 * which may then be retrieved by calling the ->event() hook with the event
 * numbers. Required.
 */
  static int fio_librgw_getevents(struct thread_data *td, unsigned int min,
				  unsigned int max, const struct timespec *t)
  {
    return 0;
  }

/*
 * The ->cancel() hook attempts to cancel the io_u. Only relevant for
 * async io engines, and need not be supported.
 */
  static int fio_librgw_cancel(struct thread_data *td, struct io_u *io_u)
  {
    return 0;
  }

/*
 * The ->queue() hook is responsible for initiating io on the io_u
 * being passed in. If the io engine is a synchronous one, io may complete
 * before ->queue() returns. Required.
 *
 * The io engine must transfer in the direction noted by io_u->ddir
 * to the buffer pointed to by io_u->xfer_buf for as many bytes as
 * io_u->xfer_buflen. Residual data count may be set in io_u->resid
 * for a short read/write.
 */
  static enum fio_q_status fio_librgw_queue(struct thread_data *td,
					    struct io_u *io_u)
  {
    librgw_data* data = static_cast<librgw_data*>(td->io_ops_data);
    const char* object = io_u->file->file_name;
    struct rgw_file_handle* object_fh = nullptr;
    size_t nbytes;
    int r = 0;

    /*
     * Double sanity check to catch errant write on a readonly setup
     */
    fio_ro_check(td, io_u);

    if (io_u->ddir == DDIR_WRITE) {
      /* Do full write cycle */
      r = rgw_lookup(data->fs, data->bucket_fh, object, &object_fh, nullptr, 0,
		     RGW_LOOKUP_FLAG_CREATE);
      if (!! r) {
	dprint(FD_IO, "rgw_lookup failed to create filehandle for %s\n",
	       object);
	goto out;
      }

      r = rgw_open(data->fs, object_fh, 0 /* posix flags */, 0 /* flags */);
      if (!! r) {
	dprint(FD_IO, "rgw_open failed to create filehandle for %s\n",
	       object);
	rgw_fh_rele(data->fs, object_fh, RGW_FH_RELE_FLAG_NONE);
	goto out;
      }

      /* librgw can write at any offset, but only sequentially
       * starting at 0, in one open/write/close cycle */
      r = rgw_write(data->fs, object_fh, 0, io_u->xfer_buflen, &nbytes,
		    (void*) io_u->xfer_buf, RGW_WRITE_FLAG_NONE);
      if (!! r) {
	dprint(FD_IO, "rgw_write failed for %s\n",
	       object);
      }

      r = rgw_close(data->fs, object_fh, 0 /* flags */);

      /* object_fh is closed but still reachable, save it */
      data->save_handle(object_fh);
    } else if (io_u->ddir == DDIR_READ) {

      r = rgw_lookup(data->fs, data->bucket_fh, object, &object_fh,
			nullptr, 0, RGW_LOOKUP_FLAG_NONE);
      if (!! r) {
	dprint(FD_IO, "rgw_lookup failed to create filehandle for %s\n",
	       object);
	goto out;
      }

      r = rgw_open(data->fs, object_fh, 0 /* posix flags */, 0 /* flags */);
      if (!! r) {
	dprint(FD_IO, "rgw_open failed to create filehandle for %s\n",
	       object);
	rgw_fh_rele(data->fs, object_fh, RGW_FH_RELE_FLAG_NONE);
	goto out;
      }

      r = rgw_read(data->fs, object_fh, io_u->offset, io_u->xfer_buflen,
		   &nbytes, io_u->xfer_buf, RGW_READ_FLAG_NONE);
      if (!! r) {
	dprint(FD_IO, "rgw_read failed for %s\n",
	       object);
      }
    } else {
      dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
	     io_u->ddir);
    }

    if (object_fh) {
      r = rgw_close(data->fs, object_fh, 0 /* flags */);

      /* object_fh is closed but still reachable, save it */
      data->save_handle(object_fh);
    }

  out:
    /*
     * Could return FIO_Q_QUEUED for a queued request,
     * FIO_Q_COMPLETED for a completed request, and FIO_Q_BUSY
     * if we could queue no more at this point (you'd have to
     * define ->commit() to handle that.
     */
    return FIO_Q_COMPLETED;
  }

  int fio_librgw_commit(thread_data* td)
  {
    // commit() allows the engine to batch up queued requests to be submitted all
    // at once. it would be natural for queue() to collect transactions in a list,
    // and use commit() to pass them all to ObjectStore::queue_transactions(). but
    // because we spread objects over multiple collections, we a) need to use a
    // different sequencer for each collection, and b) are less likely to see a
    // benefit from batching requests within a collection
    return 0;
  }

/*
 * Hook for opening the given file. Unless the engine has special
 * needs, it usually just provides generic_open_file() as the handler.
 */
  static int fio_librgw_open(struct thread_data *td, struct fio_file *f)
  {
    /* for now, let's try to avoid doing open/close in these hooks */
    return 0;
  }

/*
 * Hook for closing a file. See fio_librgw_open().
 */
  static int fio_librgw_close(struct thread_data *td, struct fio_file *f)
  {
    /* for now, let's try to avoid doing open/close in these hooks */
    return 0;
  }

/* XXX next two probably not needed */
  int fio_librgw_io_u_init(thread_data* td, io_u* u)
  {
    // no data is allocated, we just use the pointer as a boolean 'completed' flag
    u->engine_data = nullptr;
    return 0;
  }

  void fio_librgw_io_u_free(thread_data* td, io_u* u)
  {
    u->engine_data = nullptr;
  }

  struct librgw_ioengine : public ioengine_ops 
  {
    librgw_ioengine() : ioengine_ops({}) {
      name        = "librgw";
      version     = FIO_IOOPS_VERSION;
      flags       = FIO_DISKLESSIO;
      setup       = fio_librgw_setup;
      init        = fio_librgw_init;
      queue       = fio_librgw_queue;
      commit      = fio_librgw_commit;
      getevents   = fio_librgw_getevents;
      event       = fio_librgw_event;
      cleanup     = fio_librgw_cleanup;
      open_file   = fio_librgw_open;
      close_file  = fio_librgw_close;
      io_u_init   = fio_librgw_io_u_init;
      io_u_free   = fio_librgw_io_u_free;
      options     = ::options.data();
      option_struct_size = sizeof(opt_struct);
    }
  };

} // namespace

extern "C" {
// the exported fio engine interface
  void get_ioengine(struct ioengine_ops** ioengine_ptr) {
    static librgw_ioengine ioengine;
    *ioengine_ptr = &ioengine;
  }
} // extern "C"
