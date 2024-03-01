// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 *  CEPH messenger engine
 *
 *  FIO engine which uses ceph messenger as a transport.  See corresponding
 *  FIO client and server jobs for details.
 */

#include "global/global_init.h"
#include "msg/Messenger.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "common/perf_counters.h"
#include "auth/DummyAuth.h"
#include "ring_buffer.h"

#include <fio.h>
#include <flist.h>
#include <optgroup.h>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

using namespace std;

enum ceph_msgr_type {
  CEPH_MSGR_TYPE_UNDEF,
  CEPH_MSGR_TYPE_POSIX,
  CEPH_MSGR_TYPE_DPDK,
  CEPH_MSGR_TYPE_RDMA,
};

const char *ceph_msgr_types[] = { "undef", "async+posix",
				  "async+dpdk", "async+rdma" };

struct ceph_msgr_options {
  struct thread_data *td__;
  unsigned int is_receiver;
  unsigned int is_single;
  unsigned int port;
  const char *hostname;
  const char *conffile;
  enum ceph_msgr_type ms_type;
};

class FioDispatcher;

struct ceph_msgr_data {
  ceph_msgr_data(struct ceph_msgr_options *o_, unsigned iodepth) :
    o(o_) {
    INIT_FLIST_HEAD(&io_inflight_list);
    INIT_FLIST_HEAD(&io_pending_list);
    ring_buffer_init(&io_completed_q, iodepth);
    pthread_spin_init(&spin, PTHREAD_PROCESS_PRIVATE);
  }

  struct ceph_msgr_options *o;
  Messenger *msgr = NULL;
  FioDispatcher *disp = NULL;
  pthread_spinlock_t spin;
  struct ring_buffer io_completed_q;
  struct flist_head io_inflight_list;
  struct flist_head io_pending_list;
  unsigned int io_inflight_nr = 0;
  unsigned int io_pending_nr  = 0;
};

struct ceph_msgr_io {
  struct flist_head list;
  struct ceph_msgr_data *data;
  struct io_u *io_u;
  MOSDOp *req_msg; /** Cached request, valid only for sender */
};

struct ceph_msgr_reply_io {
  struct flist_head list;
  MOSDOpReply *rep;
};

static void *str_to_ptr(const std::string &str)
{
  // str is assumed to be a valid ptr string
  return reinterpret_cast<void*>(ceph::parse<uintptr_t>(str, 16).value());
}

static std::string ptr_to_str(void *ptr)
{
  char buf[32];

  snprintf(buf, sizeof(buf), "%llx", (unsigned long long)ptr);
  return std::string(buf);
}

/*
 * Used for refcounters print on the last context put, almost duplicates
 * global context refcounter, sigh.
 */
static std::atomic<int> ctx_ref(1);
static DummyAuthClientServer *g_dummy_auth;

static void create_or_get_ceph_context(struct ceph_msgr_options *o)
{
  if (g_ceph_context) {
    g_ceph_context->get();
    ctx_ref++;
    return;
  }

  boost::intrusive_ptr<CephContext> cct;
  vector<const char*> args;

  if (o->conffile)
    args = { "--conf", o->conffile };

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY,
		    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  /* Will use g_ceph_context instead */
  cct.detach();

  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(NULL);
  g_dummy_auth = new DummyAuthClientServer(g_ceph_context);
  g_dummy_auth->auth_registry.refresh_config();
}

static void put_ceph_context(void)
{
  if (--ctx_ref == 0) {
    ostringstream ostr;
    Formatter* f;

    f = Formatter::create("json-pretty");
    g_ceph_context->get_perfcounters_collection()->dump_formatted(f, false, false);
    ostr << ">>>>>>>>>>>>> PERFCOUNTERS BEGIN <<<<<<<<<<<<" << std::endl;
    f->flush(ostr);
    ostr << ">>>>>>>>>>>>>  PERFCOUNTERS END  <<<<<<<<<<<<" << std::endl;

    delete f;
    delete g_dummy_auth;
    dout(0) <<  ostr.str() << dendl;
  }

  g_ceph_context->put();
}

static void ceph_msgr_sender_on_reply(const object_t &oid)
{
  struct ceph_msgr_data *data;
  struct ceph_msgr_io *io;

  /*
   * Here we abuse object and use it as a raw pointer. Since this is
   * only for benchmarks and testing we do not care about anything
   * but performance.  So no need to use global structure in order
   * to search for reply, just send a pointer and get it back.
   */

  io = (decltype(io))str_to_ptr(oid.name);
  data = io->data;
  ring_buffer_enqueue(&data->io_completed_q, (void *)io);
}


class ReplyCompletion : public Message::CompletionHook {
  struct ceph_msgr_io *m_io;

public:
  ReplyCompletion(MOSDOpReply *rep, struct ceph_msgr_io *io) :
    Message::CompletionHook(rep),
    m_io(io) {
  }
  void finish(int err) override {
    struct ceph_msgr_data *data = m_io->data;

    ring_buffer_enqueue(&data->io_completed_q, (void *)m_io);
  }
};

static void ceph_msgr_receiver_on_request(struct ceph_msgr_data *data,
					  MOSDOp *req)
{
  MOSDOpReply *rep;

  rep = new MOSDOpReply(req, 0, 0, 0, false);
  rep->set_connection(req->get_connection());

  pthread_spin_lock(&data->spin);
  if (data->io_inflight_nr) {
    struct ceph_msgr_io *io;

    data->io_inflight_nr--;
    io = flist_first_entry(&data->io_inflight_list,
			   struct ceph_msgr_io, list);
    flist_del(&io->list);
    pthread_spin_unlock(&data->spin);

    rep->set_completion_hook(new ReplyCompletion(rep, io));
    rep->get_connection()->send_message(rep);
  } else {
    struct ceph_msgr_reply_io *rep_io;

    rep_io = (decltype(rep_io))malloc(sizeof(*rep_io));
    rep_io->rep = rep;

    data->io_pending_nr++;
    flist_add_tail(&rep_io->list, &data->io_pending_list);
    pthread_spin_unlock(&data->spin);
  }
}

class FioDispatcher : public Dispatcher {
  struct ceph_msgr_data *m_data;

public:
  FioDispatcher(struct ceph_msgr_data *data):
    Dispatcher(g_ceph_context),
    m_data(data) {
  }
  bool ms_can_fast_dispatch_any() const override {
    return true;
  }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OP:
      return m_data->o->is_receiver;
    case CEPH_MSG_OSD_OPREPLY:
      return !m_data->o->is_receiver;
    default:
      return false;
    }
  }
  void ms_handle_fast_connect(Connection *con) override {
  }
  void ms_handle_fast_accept(Connection *con) override {
  }
  bool ms_dispatch(Message *m) override {
    return true;
  }
  void ms_fast_dispatch(Message *m) override {
    if (m_data->o->is_receiver) {
      MOSDOp *req;

      /*
       * Server side, handle request.
       */

      req = static_cast<MOSDOp*>(m);
      req->finish_decode();

      ceph_msgr_receiver_on_request(m_data, req);
    } else {
      MOSDOpReply *rep;

      /*
       * Client side, get reply, extract objid and mark
       * IO as completed.
       */

      rep = static_cast<MOSDOpReply*>(m);
      ceph_msgr_sender_on_reply(rep->get_oid());
    }
    m->put();
  }
  bool ms_handle_reset(Connection *con) override {
    return true;
  }
  void ms_handle_remote_reset(Connection *con) override {
  }
  bool ms_handle_refused(Connection *con) override {
    return false;
  }
  int ms_handle_fast_authentication(Connection *con) override {
    return 1;
  }
};

static entity_addr_t hostname_to_addr(struct ceph_msgr_options *o)
{
  entity_addr_t addr;

  addr.parse(o->hostname);
  addr.set_port(o->port);
  addr.set_nonce(0);

  return addr;
}

static Messenger *create_messenger(struct ceph_msgr_options *o)
{
  entity_name_t ename = o->is_receiver ?
    entity_name_t::OSD(0) : entity_name_t::CLIENT(0);
  std::string lname = o->is_receiver ?
    "receiver" : "sender";

  std::string ms_type = o->ms_type != CEPH_MSGR_TYPE_UNDEF ?
    ceph_msgr_types[o->ms_type] :
    g_ceph_context->_conf.get_val<std::string>("ms_type");

  /* o->td__>pid doesn't set value, so use getpid() instead*/
  auto nonce = o->is_receiver ? 0 : (getpid() + o->td__->thread_number);
  Messenger *msgr = Messenger::create(g_ceph_context, ms_type.c_str(),
				      ename, lname, nonce);
  if (o->is_receiver) {
    msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    msgr->bind(hostname_to_addr(o));
  } else {
    msgr->set_default_policy(Messenger::Policy::lossless_client(0));
  }
  msgr->set_auth_client(g_dummy_auth);
  msgr->set_auth_server(g_dummy_auth);
  msgr->set_require_authorizer(false);
  msgr->start();

  return msgr;
}

static Messenger *single_msgr;
static std::atomic<int> single_msgr_ref;
static vector<FioDispatcher *> single_msgr_disps;

static void init_messenger(struct ceph_msgr_data *data)
{
  struct ceph_msgr_options *o = data->o;
  FioDispatcher *disp;
  Messenger *msgr;

  disp = new FioDispatcher(data);
  if (o->is_single) {
    /*
     * Single messenger instance for the whole FIO
     */

    if (!single_msgr) {
      msgr = create_messenger(o);
      single_msgr = msgr;
    } else {
      msgr = single_msgr;
    }
    single_msgr_disps.push_back(disp);
    single_msgr_ref++;
  } else {
    /*
     * Messenger instance per FIO thread
     */
    msgr = create_messenger(o);
  }
  msgr->add_dispatcher_head(disp);

  data->disp = disp;
  data->msgr = msgr;
}

static void free_messenger(struct ceph_msgr_data *data)
{
  data->msgr->shutdown();
  data->msgr->wait();
  delete data->msgr;
}

static void put_messenger(struct ceph_msgr_data *data)
{
  struct ceph_msgr_options *o = data->o;

  if (o->is_single) {
    if (--single_msgr_ref == 0) {
      free_messenger(data);
      /*
       * In case of a single messenger instance we have to
       * free dispatchers after actual messenger destruction.
       */
      for (auto disp : single_msgr_disps)
	delete disp;
      single_msgr = NULL;
    }
  } else {
    free_messenger(data);
    delete data->disp;
  }
  data->disp = NULL;
  data->msgr = NULL;
}

static int fio_ceph_msgr_setup(struct thread_data *td)
{
  struct ceph_msgr_options *o = (decltype(o))td->eo;
  o->td__ = td;
  ceph_msgr_data *data;

  /* We have to manage global resources so we use threads */
  td->o.use_thread = 1;

  create_or_get_ceph_context(o);

  if (!td->io_ops_data) {
    data = new ceph_msgr_data(o, td->o.iodepth);
    init_messenger(data);
    td->io_ops_data = (void *)data;
  }

  return 0;
}

static void fio_ceph_msgr_cleanup(struct thread_data *td)
{
  struct ceph_msgr_data *data;
  unsigned nr;

  data = (decltype(data))td->io_ops_data;
  put_messenger(data);

  nr = ring_buffer_used_size(&data->io_completed_q);
  if (nr)
    fprintf(stderr, "fio: io_completed_nr==%d, but should be zero\n",
	    nr);
  if (data->io_inflight_nr)
    fprintf(stderr, "fio: io_inflight_nr==%d, but should be zero\n",
	    data->io_inflight_nr);
  if (data->io_pending_nr)
    fprintf(stderr, "fio: io_pending_nr==%d, but should be zero\n",
	    data->io_pending_nr);
  if (!flist_empty(&data->io_inflight_list))
    fprintf(stderr, "fio: io_inflight_list is not empty\n");
  if (!flist_empty(&data->io_pending_list))
    fprintf(stderr, "fio: io_pending_list is not empty\n");

  ring_buffer_deinit(&data->io_completed_q);
  delete data;
  put_ceph_context();
}

static int fio_ceph_msgr_io_u_init(struct thread_data *td, struct io_u *io_u)
{
  struct ceph_msgr_options *o = (decltype(o))td->eo;
  struct ceph_msgr_io *io;
  MOSDOp *req_msg = NULL;

  io = (decltype(io))malloc(sizeof(*io));
  io->io_u = io_u;
  io->data = (decltype(io->data))td->io_ops_data;

  if (!o->is_receiver) {
    object_t oid(ptr_to_str(io));
    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(oid, oloc.key, CEPH_NOSNAP, pgid.ps(),
		   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    entity_inst_t dest(entity_name_t::OSD(0), hostname_to_addr(o));

    Messenger *msgr = io->data->msgr;
    ConnectionRef con = msgr->connect_to(dest.name.type(),
					 entity_addrvec_t(dest.addr));

    req_msg = new MOSDOp(0, 0, hobj, spgid, 0, 0, 0);
    req_msg->set_connection(con);
  }

  io->req_msg = req_msg;
  io_u->engine_data = (void *)io;

  return 0;
}

static void fio_ceph_msgr_io_u_free(struct thread_data *td, struct io_u *io_u)
{
  struct ceph_msgr_io *io;

  io = (decltype(io))io_u->engine_data;
  if (io) {
    io_u->engine_data = NULL;
    if (io->req_msg)
      io->req_msg->put();
    free(io);
  }
}

static enum fio_q_status ceph_msgr_sender_queue(struct thread_data *td,
						struct io_u *io_u)
{
  struct ceph_msgr_data *data;
  struct ceph_msgr_io *io;

  bufferlist buflist = bufferlist::static_from_mem(
    (char *)io_u->buf, io_u->buflen);

  io = (decltype(io))io_u->engine_data;
  data = (decltype(data))td->io_ops_data;

  /* No handy method to clear ops before reusage? Ok */
  io->req_msg->ops.clear();

  /* Here we do not care about direction, always send as write */
  io->req_msg->write(0, io_u->buflen, buflist);
  /* Keep message alive */
  io->req_msg->get();
  io->req_msg->get_connection()->send_message(io->req_msg);

  return FIO_Q_QUEUED;
}

static int fio_ceph_msgr_getevents(struct thread_data *td, unsigned int min,
				   unsigned int max, const struct timespec *ts)
{
  struct ceph_msgr_data *data;
  unsigned int nr;

  data = (decltype(data))td->io_ops_data;

  /*
   * Check io_u.c : if min == 0 -> ts is valid and equal to zero,
   * if min != 0 -> ts is NULL.
   */
  assert(!min ^ !ts);

  nr = ring_buffer_used_size(&data->io_completed_q);
  if (nr >= min)
    /* We got something */
    return min(nr, max);

  /* Here we are only if min != 0 and ts == NULL */
  assert(min && !ts);

  while ((nr = ring_buffer_used_size(&data->io_completed_q)) < min &&
	 !td->terminate) {
    /* Poll, no disk IO, so we expect response immediately. */
    usleep(10);
  }

  return min(nr, max);
}

static struct io_u *fio_ceph_msgr_event(struct thread_data *td, int event)
{
  struct ceph_msgr_data *data;
  struct ceph_msgr_io *io;

  data = (decltype(data))td->io_ops_data;
  io = (decltype(io))ring_buffer_dequeue(&data->io_completed_q);

  return io->io_u;
}

static enum fio_q_status ceph_msgr_receiver_queue(struct thread_data *td,
						  struct io_u *io_u)
{
  struct ceph_msgr_data *data;
  struct ceph_msgr_io *io;

  io = (decltype(io))io_u->engine_data;
  data = io->data;
  pthread_spin_lock(&data->spin);
  if (data->io_pending_nr) {
    struct ceph_msgr_reply_io *rep_io;
    MOSDOpReply *rep;

    data->io_pending_nr--;
    rep_io = flist_first_entry(&data->io_pending_list,
			       struct ceph_msgr_reply_io,
			       list);
    flist_del(&rep_io->list);
    rep = rep_io->rep;
    pthread_spin_unlock(&data->spin);
    free(rep_io);

    rep->set_completion_hook(new ReplyCompletion(rep, io));
    rep->get_connection()->send_message(rep);
  } else {
    data->io_inflight_nr++;
    flist_add_tail(&io->list, &data->io_inflight_list);
    pthread_spin_unlock(&data->spin);
  }

  return FIO_Q_QUEUED;
}

static enum fio_q_status fio_ceph_msgr_queue(struct thread_data *td,
					     struct io_u *io_u)
{
  struct ceph_msgr_options *o = (decltype(o))td->eo;

  if (o->is_receiver)
    return ceph_msgr_receiver_queue(td, io_u);
  else
    return ceph_msgr_sender_queue(td, io_u);
}

static int fio_ceph_msgr_open_file(struct thread_data *td, struct fio_file *f)
{
  return 0;
}

static int fio_ceph_msgr_close_file(struct thread_data *, struct fio_file *)
{
  return 0;
}

template <class Func>
fio_option make_option(Func&& func)
{
  auto o = fio_option{};
  o.category = FIO_OPT_C_ENGINE;
  func(std::ref(o));
  return o;
}

static std::vector<fio_option> options {
  make_option([] (fio_option& o) {
    o.name  = "receiver";
    o.lname = "CEPH messenger is receiver";
    o.type  = FIO_OPT_BOOL;
    o.off1  = offsetof(struct ceph_msgr_options, is_receiver);
    o.help  = "CEPH messenger is sender or receiver";
    o.def   = "0";
  }),
  make_option([] (fio_option& o) {
    o.name  = "single_instance";
    o.lname = "Single instance of CEPH messenger ";
    o.type  = FIO_OPT_BOOL;
    o.off1  = offsetof(struct ceph_msgr_options, is_single);
    o.help  = "CEPH messenger is a created once for all threads";
    o.def   = "0";
  }),
  make_option([] (fio_option& o) {
    o.name  = "hostname";
    o.lname = "CEPH messenger hostname";
    o.type  = FIO_OPT_STR_STORE;
    o.off1  = offsetof(struct ceph_msgr_options, hostname);
    o.help  = "Hostname for CEPH messenger engine";
  }),
  make_option([] (fio_option& o) {
    o.name   = "port";
    o.lname  = "CEPH messenger engine port";
    o.type   = FIO_OPT_INT;
    o.off1   = offsetof(struct ceph_msgr_options, port);
    o.maxval = 65535;
    o.minval = 1;
    o.help   = "Port to use for CEPH messenger";
  }),
  make_option([] (fio_option& o) {
    o.name  = "ms_type";
    o.lname = "CEPH messenger transport type: async+posix, async+dpdk, async+rdma";
    o.type  = FIO_OPT_STR;
    o.off1  = offsetof(struct ceph_msgr_options, ms_type);
    o.help  = "Transport type for CEPH messenger, see 'ms async transport type' corresponding CEPH documentation page";
    o.def   = "undef";

    o.posval[0].ival = "undef";
    o.posval[0].oval = CEPH_MSGR_TYPE_UNDEF;

    o.posval[1].ival = "async+posix";
    o.posval[1].oval = CEPH_MSGR_TYPE_POSIX;
    o.posval[1].help = "POSIX API";

    o.posval[2].ival = "async+dpdk";
    o.posval[2].oval = CEPH_MSGR_TYPE_DPDK;
    o.posval[2].help = "DPDK";

    o.posval[3].ival = "async+rdma";
    o.posval[3].oval = CEPH_MSGR_TYPE_RDMA;
    o.posval[3].help = "RDMA";
  }),
  make_option([] (fio_option& o) {
    o.name  = "ceph_conf_file";
    o.lname = "CEPH configuration file";
    o.type  = FIO_OPT_STR_STORE;
    o.off1  = offsetof(struct ceph_msgr_options, conffile);
    o.help  = "Path to CEPH configuration file";
  }),
  {} /* Last NULL */
};

static struct ioengine_ops ioengine;

extern "C" {

void get_ioengine(struct ioengine_ops** ioengine_ptr)
{
  /*
   * Main ioengine structure
   */
  ioengine.name	= "ceph-msgr";
  ioengine.version	= FIO_IOOPS_VERSION;
  ioengine.flags	= FIO_DISKLESSIO | FIO_UNIDIR | FIO_PIPEIO;
  ioengine.setup	= fio_ceph_msgr_setup;
  ioengine.queue	= fio_ceph_msgr_queue;
  ioengine.getevents	= fio_ceph_msgr_getevents;
  ioengine.event	= fio_ceph_msgr_event;
  ioengine.cleanup	= fio_ceph_msgr_cleanup;
  ioengine.open_file	= fio_ceph_msgr_open_file;
  ioengine.close_file	= fio_ceph_msgr_close_file;
  ioengine.io_u_init	= fio_ceph_msgr_io_u_init;
  ioengine.io_u_free	= fio_ceph_msgr_io_u_free;
  ioengine.option_struct_size = sizeof(struct ceph_msgr_options);
  ioengine.options	= options.data();

  *ioengine_ptr = &ioengine;
}
} // extern "C"
