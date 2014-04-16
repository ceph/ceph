/*
 *  Ceph FileStore engine
 *
 * IO engine using Ceph's FileStore class to test low-level performance of
 * Ceph OSDs.
 *
 */

#include "os/FileStore.h"
#include "global/global_init.h"

/* silence some fio.h include issues.
 * fio.h needs to made robust for third-party software.
 */
#undef ARRAY_SIZE /* double declartion */
#define CONFIG_CPU_COUNT /* double declaration: fio.h should check if CPU_COUNT alread got defined */
#undef le16_to_cpu
#undef le32_to_cpu
#undef le64_to_cpu

#include "fio.h"

struct fio_ceph_filestore_iou {
	struct io_u *io_u;
	int io_complete;
};

struct ceph_filestore_data {
	struct io_u **aio_events;
	char *osd_path;
	char *journal_path;
	ObjectStore *fs;
};

struct ceph_filestore_options {
	struct thread_data *td;
	char *ceph_filestore_name;
	char *pool_name;
	char *client_name;
};

#if 0
static struct fio_option options[] = {
	{
	 .name     = "ceph_filestorename",
	 .lname    = "ceph_filestore engine ceph_filestorename",
	 .type     = FIO_OPT_STR_STORE,
	 .help     = "RBD name for RBD engine",
	 .off1     = offsetof(struct ceph_filestore_options, ceph_filestore_name),
	 .category = FIO_OPT_C_ENGINE,
	 .group    = FIO_OPT_G_RBD,
	 },
	{
	 .name     = "pool",
	 .lname    = "ceph_filestore engine pool",
	 .type     = FIO_OPT_STR_STORE,
	 .help     = "Name of the pool hosting the RBD for the RBD engine",
	 .off1     = offsetof(struct ceph_filestore_options, pool_name),
	 .category = FIO_OPT_C_ENGINE,
	 .group    = FIO_OPT_G_RBD,
	 },
	{
	 .name     = "clientname",
	 .lname    = "ceph_filestore engine clientname",
	 .type     = FIO_OPT_STR_STORE,
	 .help     = "Name of the ceph client to access the RBD for the RBD engine",
	 .off1     = offsetof(struct ceph_filestore_options, client_name),
	 .category = FIO_OPT_C_ENGINE,
	 .group    = FIO_OPT_G_RBD,
	 },
	{
	 .name = NULL,
	 },
};
#endif

/////////////////////////////


struct OnCommitted : public Context {
  struct io_u *io_u;
  OnCommitted(struct io_u* io_u) : io_u(io_u) {}
  void finish(int r) {
  }
};

struct OnApplied : public Context {
  struct io_u *io_u;
  ObjectStore::Transaction *t;
  OnApplied(struct io_u* io_u, ObjectStore::Transaction *t) : io_u(io_u), t(t) {}
  void finish(int r) {

	struct fio_ceph_filestore_iou *fio_ceph_filestore_iou =
	    (struct fio_ceph_filestore_iou *)io_u->engine_data;

	fio_ceph_filestore_iou->io_complete = 1;


	delete t;
  }
};




static int _fio_setup_ceph_filestore_data(struct thread_data *td,
			       struct ceph_filestore_data **ceph_filestore_data_ptr)
{
	struct ceph_filestore_data *ceph_filestore_data;

	if (td->io_ops->data)
		return 0;

	ceph_filestore_data = (struct ceph_filestore_data*) malloc(sizeof(struct ceph_filestore_data));
	if (!ceph_filestore_data)
		goto failed;

	memset(ceph_filestore_data, 0, sizeof(struct ceph_filestore_data));

	ceph_filestore_data->aio_events = (struct io_u **) malloc(td->o.iodepth * sizeof(struct io_u *));
	if (!ceph_filestore_data->aio_events)
		goto failed;

	memset(ceph_filestore_data->aio_events, 0, td->o.iodepth * sizeof(struct io_u *));

	*ceph_filestore_data_ptr = ceph_filestore_data;

	return 0;

failed:
	return 1;

}

static struct io_u *fio_ceph_filestore_event(struct thread_data *td, int event)
{
	struct ceph_filestore_data *ceph_filestore_data = (struct ceph_filestore_data *) td->io_ops->data;

	return ceph_filestore_data->aio_events[event];
}

static int fio_ceph_filestore_getevents(struct thread_data *td, unsigned int min,
			     unsigned int max, struct timespec *t)
{
	struct ceph_filestore_data *ceph_filestore_data = (struct ceph_filestore_data *) td->io_ops->data;
	unsigned int events = 0;
	struct io_u *io_u;
	unsigned int i;
	struct fio_ceph_filestore_iou *fov;

	do {
		io_u_qiter(&td->io_u_all, io_u, i) {
			if (!(io_u->flags & IO_U_F_FLIGHT))
				continue;

			fov = (struct fio_ceph_filestore_iou *)io_u->engine_data;

			if (fov->io_complete) {
				fov->io_complete = 0;
				ceph_filestore_data->aio_events[events] = io_u;
				events++;
			}

		}
		if (events < min)
			usleep(100);
		else
			break;

	} while (1);

	return events;
}

static int fio_ceph_filestore_queue(struct thread_data *td, struct io_u *io_u)
{
	int r = -1;
	char buf[32];
	struct ceph_filestore_data *ceph_filestore_data = (struct ceph_filestore_data *) td->io_ops->data;
	uint64_t len = io_u->xfer_buflen;
	uint64_t off = io_u->offset;
	ObjectStore *fs = ceph_filestore_data->fs;
	object_t poid(buf);

	bufferlist data;
	snprintf(buf, sizeof(buf), "XXX_%lu_%lu", io_u->start_time.tv_usec, io_u->start_time.tv_sec);
	data.append((char *)io_u->xfer_buf, io_u->xfer_buflen);

	fio_ro_check(td, io_u);


	ObjectStore::Transaction *t = new ObjectStore::Transaction;
	if (!t) {

		cout << "ObjectStore Transcation allocation failed." << std::endl;
		goto failed;
	}


        if (io_u->ddir == DDIR_WRITE) {
		t->write(coll_t(), hobject_t(poid), off, len, data);
		//cout << "QUEUING transaction " << io_u << std::endl;
		fs->queue_transaction(NULL, t, new OnApplied(io_u, t), new OnCommitted(io_u));
	} else {
		cout << "WARNING: No DDIR beside DDIR_WRITE supported!" << std::endl;
		return FIO_Q_COMPLETED;
	}

	return FIO_Q_QUEUED;

failed:
	io_u->error = r;
	td_verror(td, io_u->error, "xfer");
	return FIO_Q_COMPLETED;
}

static int fio_ceph_filestore_init(struct thread_data *td)
{
	vector<const char*> args;
	struct ceph_filestore_data *ceph_filestore_data = (struct ceph_filestore_data *) td->io_ops->data;
	ObjectStore::Transaction ft;

	global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, 0);
	//g_conf->journal_dio = false;
	common_init_finish(g_ceph_context);
	//g_ceph_context->_conf->set_val("debug_filestore", "20");
	//g_ceph_context->_conf->set_val("debug_throttle", "20");
	g_ceph_context->_conf->apply_changes(NULL);

	ceph_filestore_data->osd_path = strdup("/mnt/fio_ceph_filestore.XXXXXXX");
	ceph_filestore_data->journal_path = strdup("/var/lib/ceph/osd/journal-ram/fio_ceph_filestore.XXXXXXX");

	mkdtemp(ceph_filestore_data->osd_path);
	//mktemp(ceph_filestore_data->journal_path); // NOSPC issue

  	ObjectStore *fs = new FileStore(ceph_filestore_data->osd_path, ceph_filestore_data->journal_path);
	ceph_filestore_data->fs = fs;

	if (fs->mkfs() < 0) {
		cout << "mkfs failed" << std::endl;
		goto failed;
	}
	
	if (fs->mount() < 0) {
		cout << "mount failed" << std::endl;
		goto failed;
	}

	ft.create_collection(coll_t());
	fs->apply_transaction(ft);


	return 0;

failed:
	return 1;

}

static void fio_ceph_filestore_cleanup(struct thread_data *td)
{
	struct ceph_filestore_data *ceph_filestore_data = (struct ceph_filestore_data *) td->io_ops->data;

	if (ceph_filestore_data) {
		free(ceph_filestore_data->aio_events);
		free(ceph_filestore_data->osd_path);
		free(ceph_filestore_data->journal_path);
		free(ceph_filestore_data);
	}

}

static int fio_ceph_filestore_setup(struct thread_data *td)
{
	int r = 0;
	struct fio_file *f;
	struct ceph_filestore_data *ceph_filestore_data = NULL;

	/* allocate engine specific structure to deal with libceph_filestore. */
	r = _fio_setup_ceph_filestore_data(td, &ceph_filestore_data);
	if (r) {
		log_err("fio_setup_ceph_filestore_data failed.\n");
		goto cleanup;
	}
	td->io_ops->data = ceph_filestore_data;

	/* taken from "net" engine. Pretend we deal with files,
	 * even if we do not have any ideas about files.
	 * The size of the FileStore is set instead of a artificial file.
	 */
	if (!td->files_index) {
		add_file(td, td->o.filename ? : "ceph_filestore", 0, 0);
		td->o.nr_files = td->o.nr_files ? : 1;
	}
	f = td->files[0];
	f->real_file_size = 1024 * 1024; 

	return 0;

cleanup:
	fio_ceph_filestore_cleanup(td);
	return r;
}

static int fio_ceph_filestore_open(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static void fio_ceph_filestore_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct fio_ceph_filestore_iou *o = (struct fio_ceph_filestore_iou *) io_u->engine_data;

	if (o) {
		io_u->engine_data = NULL;
		free(o);
	}
}

static int fio_ceph_filestore_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct fio_ceph_filestore_iou *o;

	o = (struct fio_ceph_filestore_iou *) malloc(sizeof(*o));
	o->io_complete = 0;
	o->io_u = io_u;
	io_u->engine_data = o;
	return 0;
}

extern "C" {
void get_ioengine(struct ioengine_ops **ioengine_ptr) {
	struct ioengine_ops *ioengine;
	*ioengine_ptr = (struct ioengine_ops *) malloc(sizeof(struct ioengine_ops));
	ioengine = *ioengine_ptr;

	strcpy(ioengine->name, "ceph_filestore");
	ioengine->version        = FIO_IOOPS_VERSION;
	ioengine->setup          = fio_ceph_filestore_setup;
	ioengine->init           = fio_ceph_filestore_init;
	//ioengine->prep           = fio_ceph_filestore_prep;
	ioengine->queue          = fio_ceph_filestore_queue;
	//ioengine->cancel         = fio_ceph_filestore_cancel;
	ioengine->getevents      = fio_ceph_filestore_getevents;
	ioengine->event          = fio_ceph_filestore_event;
	ioengine->cleanup        = fio_ceph_filestore_cleanup;
	ioengine->open_file      = fio_ceph_filestore_open;
	//ioengine->close_file     = fio_ceph_filestore_close;
	ioengine->io_u_init      = fio_ceph_filestore_io_u_init;
	ioengine->io_u_free      = fio_ceph_filestore_io_u_free;
}
}

