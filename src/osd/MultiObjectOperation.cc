#include "boost/tuple/tuple.hpp"
#include "boost/bind.hpp"
#include "PG.h"
#include "ReplicatedPG.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/config.h"
#include "include/compat.h"

#include "osdc/Objecter.h"

#include "include/assert.h"
#include "include/rados/rados_types.hpp"

#ifdef WITH_LTTNG
#include "tracing/osd.h"
#else
#define tracepoint(...)
#endif

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
template <typename T>
static ostream& _prefix(std::ostream *_dout, T *pg) {
  return *_dout << pg->gen_prefix();
}


#include <sstream>
#include <utility>

#include <errno.h>

void ReplicatedPG::MultiObjectWriteOpContext::encode(bufferlist &bl)
{
  ENCODE_START(1, 1, bl);

  __u8 m = is_master ? 1 : 0;
  ::encode(m, bl);

  ::encode(master, bl);
  ::encode(locator, bl);
  ::encode(snapc, bl);
  utime_t t;
  t = ceph::real_clock::to_ceph_timespec(mtime);
  ::encode(t, bl);
  ::encode(master_reqid, bl);

  if (is_master) {
    int obj_nums = sub_objects.size();
    ::encode(obj_nums, bl);
    for (auto &p : sub_objects)
      ::encode(p.first, bl);
  } else {
    ::encode(slave_data_bl, bl);
  }

  ENCODE_FINISH(bl);
}

void ReplicatedPG::MultiObjectWriteOpContext::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);

  __u8 m;
  ::decode(m, bl);
  if (m)
    is_master = true;

  ::decode(master, bl);
  ::decode(locator, bl);
  ::decode(snapc, bl);
  utime_t t;
  ::decode(t, bl);
  mtime = ceph::real_clock::from_ceph_timespec(t);
  ::decode(master_reqid, bl);

  if (is_master) {
    int obj_nums;
    ::decode(obj_nums, bl);
    assert(obj_nums > 0);
    for (int i = 0; i < obj_nums; ++i) {
      object_t obj;
      ::decode(obj, bl);
      sub_objects.insert(make_pair(obj, make_pair(0, 0)));
    }
  } else {
    ::decode(slave_data_bl, bl);
  }

  DECODE_FINISH(bl);
}

ReplicatedPG::MultiObjectWriteOpContextRef
ReplicatedPG::multi_object_write_op_create(hobject_t oid, osd_reqid_t reqid)
{
  assert(!multi_object_write_ops.count(oid));
  MultiObjectWriteOpContextRef moc(new MultiObjectWriteOpContext);
  moc->oid = oid;
  moc->reqid = reqid;
  multi_object_write_ops[oid] = moc;
  return moc;
}

void ReplicatedPG::multi_object_write_op_destroy(MultiObjectWriteOpContextRef moc)
{
  if (moc->destroyed)
    return;

  auto i = multi_object_write_ops.find(moc->oid);
  assert(i != multi_object_write_ops.end());
  multi_object_write_ops.erase(i);

  moc->destroyed = true;
  assert(moc->waiting.empty());
}

ReplicatedPG::MultiObjectWriteOpContextRef ReplicatedPG::multi_object_write_op_find(hobject_t oid)
{
  auto it = multi_object_write_ops.find(oid);
  if (it == multi_object_write_ops.end())
    return MultiObjectWriteOpContextRef();
  return it->second;
}

void ReplicatedPG::multi_object_write_op_load(MultiObjectWriteOpContextRef moc,
                                 MultiObjectWriteOpContext::State next)
{
  if (moc->destroyed)
    return;

  hobject_t meta_oid = get_multi_object_write_op_meta_object(moc);

  if (is_degraded_or_backfilling_object(meta_oid)) {
    callbacks_for_degraded_object[meta_oid].push_back(
      new FunctionContext(boost::bind(
        &ReplicatedPG::multi_object_write_op_load, this, moc, next
      ))
    );
    maybe_kick_recovery(meta_oid);
    dout(20) << __func__ << " waiting for temp object" << dendl;
    return;
  }

  ObjectContextRef obc = get_object_context(meta_oid, false);
  if (!obc) {
    dout(1) << __func__ << " could not load meta " << meta_oid << dendl;

    requeue_ops(moc->waiting);
    multi_object_write_op_destroy(moc);
    return;
  }

  bufferlist bl;
  {
    obc->ondisk_read_lock();
    int r = osd->store->read(ch, ghobject_t(meta_oid), 0, 0, bl);
    assert(r >= 0);
    obc->ondisk_read_unlock();
  }

  bufferlist::iterator pbl = bl.begin();
  moc->decode(pbl);

  moc->state = next;
  requeue_ops(moc->waiting);

  if (moc->is_master) {
    switch (moc->state)
    {
      case MultiObjectWriteOpContext::LOCK:
        multi_object_write_op_master_unlock_slave(moc);
        break;
      case MultiObjectWriteOpContext::COMMIT:
        multi_object_write_op_master_commit_slave(moc);
        break;
      default:
        break;
    }
  } else {
    switch (moc->state)
    {
      case MultiObjectWriteOpContext::COMMIT:
        multi_object_write_op_slave_unlock_self(moc);
        break;
      default:
        break;
    }
  }
}

template <typename F>
void ReplicatedPG::multi_object_write_op_save(MultiObjectWriteOpContextRef moc, F &&f)
{
  hobject_t meta_oid = get_multi_object_write_op_meta_object(moc);

  bufferlist bl;
  moc->encode(bl);

  ObjectContextRef obc = get_object_context(meta_oid, true);
  OpContextUPtr ctx = simple_opc_create(obc);

  ctx->at_version = get_next_version();

  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::LOCK,
      moc->oid,
      ctx->at_version,
      eversion_t(),
      0,
      moc->reqid,
      ctx->mtime,
      0)
    );
  ctx->at_version.version++;

  obc->obs.oi.version = ctx->at_version;
  obc->obs.oi.mtime = ceph_clock_now(cct);;
  obc->obs.oi.size = bl.length();
  obc->obs.exists = true;
  obc->obs.oi.set_data_digest(bl.crc32c(-1));

  ctx->new_obs = obc->obs;

  obc->ssc->snapset.head_exists = true;
  ctx->new_snapset = obc->ssc->snapset;

  ctx->delta_stats.num_objects++;
  ctx->delta_stats.num_bytes += bl.length();

  bufferlist bss;
  ::encode(ctx->new_snapset, bss);
  bufferlist boi(sizeof(ctx->new_obs.oi));
  ::encode(ctx->new_obs.oi, boi, get_osdmap()->get_up_osd_features());

  ctx->op_t->append(meta_oid, 0, bl.length(), bl, 0);
  map <string, bufferlist> attrs;
  attrs[OI_ATTR].claim(boi);
  attrs[SS_ATTR].claim(bss);
  setattrs_maybe_cache(ctx->obc, ctx.get(), ctx->op_t.get(), attrs);

  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::MODIFY,
      meta_oid,
      ctx->at_version,
      eversion_t(),
      0,
      osd_reqid_t(),
      ctx->mtime,
      0)
    );

  if (pool.info.require_rollback()) {
    ctx->log.back().mod_desc.create();
  } else {
    ctx->log.back().mod_desc.mark_unrollbackable();
  }

  ctx->register_on_commit(std::move(f));

  apply_ctx_stats(ctx.get());
  simple_opc_submit(std::move(ctx));
}

void ReplicatedPG::multi_object_write_op_inline_delete(MultiObjectWriteOpContextRef moc, OpContext *ctx)
{
  hobject_t meta_oid = get_multi_object_write_op_meta_object(moc);
  assert(!is_degraded_or_backfilling_object(meta_oid));
  ObjectContextRef obc = get_object_context(meta_oid, false);
  assert(obc);

  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::UNLOCK,
      moc->oid,
      ctx->at_version,
      eversion_t(),
      0,
      moc->reqid,
      ctx->mtime,
      0)
    );
  ctx->at_version.version++;

  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::DELETE,
      meta_oid,
      ctx->at_version,
      obc->obs.oi.version,
      0,
      osd_reqid_t(),
      ctx->mtime,
      0)
    );
  ctx->at_version.version++;

  if (pool.info.require_rollback()) {
    if (ctx->log.back().mod_desc.rmobject(ctx->at_version.version)) {
      ctx->op_t->stash(meta_oid, ctx->at_version.version);
    } else {
      ctx->op_t->remove(meta_oid);
    }
  } else {
    ctx->op_t->remove(meta_oid);
    ctx->log.back().mod_desc.mark_unrollbackable();
  }

  --ctx->delta_stats.num_objects;
  ctx->delta_stats.num_bytes -= obc->obs.oi.size;
}

template <typename F>
void ReplicatedPG::multi_object_write_op_delete(MultiObjectWriteOpContextRef moc, F &&f)
{
  hobject_t meta_oid = get_multi_object_write_op_meta_object(moc);
  assert(!is_degraded_or_backfilling_object(meta_oid));
  ObjectContextRef obc = get_object_context(meta_oid, false);
  assert(obc);

  OpContextUPtr ctx = simple_opc_create(obc);
  ctx->at_version = get_next_version();
  utime_t now = ceph_clock_now(cct);
  ctx->mtime = now;

  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::UNLOCK,
      moc->oid,
      ctx->at_version,
      eversion_t(),
      0,
      moc->reqid,
      ctx->mtime,
      0)
    );
  ctx->at_version.version++;

  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::DELETE,
      meta_oid,
      ctx->at_version,
      obc->obs.oi.version,
      0,
      osd_reqid_t(),
      ctx->mtime,
      0)
    );

  if (pool.info.require_rollback()) {
    if (ctx->log.back().mod_desc.rmobject(ctx->at_version.version)) {
      ctx->op_t->stash(meta_oid, ctx->at_version.version);
    } else {
      ctx->op_t->remove(meta_oid);
    }
  } else {
    ctx->op_t->remove(meta_oid);
    ctx->log.back().mod_desc.mark_unrollbackable();
  }

  --ctx->delta_stats.num_objects;
  ctx->delta_stats.num_bytes -= obc->obs.oi.size;

  ctx->register_on_commit(std::move(f));

  apply_ctx_stats(ctx.get());
  simple_opc_submit(std::move(ctx));
}

hobject_t ReplicatedPG::get_multi_object_write_op_meta_object(MultiObjectWriteOpContextRef moc)
{
  ostringstream ss;
  ss << "moc_" << moc->reqid;
  return hobject_t(ss.str(),
                   "",
                   CEPH_NOSNAP,
		   moc->oid.get_hash(),
		   moc->oid.pool,
		   cct->_conf->osd_multi_object_operation_namespace);
}

void ReplicatedPG::multi_object_write_ops_setup()
{
  if (!is_active() || !is_primary())
    return;

  assert(multi_object_write_ops.empty());
  list<const pg_log_entry_t *> locker;
  pg_log.get_log().get_all_locker(locker);
  for (auto &it : locker) {
    MultiObjectWriteOpContextRef moc = multi_object_write_op_create(it->soid, it->reqid);
    moc->state = MultiObjectWriteOpContext::LOADING;
    MultiObjectWriteOpContext::State next = it->is_commit() ?
      MultiObjectWriteOpContext::COMMIT : MultiObjectWriteOpContext::LOCK;
    multi_object_write_op_load(moc, next);
  }
}

template <typename F1, typename F2>
void ReplicatedPG::multi_object_write_op_send(
  MultiObjectWriteOpContextRef moc,
  F1 &&fill,
  F2 &&fin)
{
  dout(20) << __func__ << *moc << dendl;
  assert(moc->is_master);

  C_GatherBuilder gather(cct);

  for (auto &p : moc->sub_objects) {
    dout(20) << __func__ << " send " << p.first << dendl;

    ObjectOperation sub_op;
    sub_op.master_reqid = moc->master_reqid;
    fill(p.first, sub_op);

    Context *sub_fin = gather.new_sub();
    int *sub_result = &p.second.first;
    p.second.second = osd->objecter->mutate(
      p.first, moc->locator,
      sub_op,
      moc->snapc, moc->mtime, 0,
      NULL,
      new FunctionContext(
        [moc, sub_result, sub_fin](int r){
          // I already hold moc ref
          *sub_result = r; // I will never use it unless fin have been called.
          sub_fin->complete(r);
        }),
      NULL);
  }

  Context *internal_fin = new C_OnFinisher(new FunctionContext(
    [this, moc, fin](int ignore){
      lock();
      for (auto &p : moc->sub_objects)
        p.second.second = 0;
      fin();
      unlock();
    }),
    &osd->objecter_finisher
  );

  if (gather.has_subs()) {
    gather.set_finisher(internal_fin);
    gather.activate();
  } else {
    internal_fin->complete(0);
  }
}

void ReplicatedPG::execute_multi_object_write_op_ctx(OpContext *ctx)
{
  OpRequestRef op = ctx->op;
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  ObjectContextRef obc = ctx->obc;
  const hobject_t& soid = obc->obs.oi.soid;
  MultiObjectWriteOpContextRef moc = multi_object_write_op_find(soid);

  if (!moc) {
    if (ctx->is_slave_commit() || ctx->is_slave_unlock()) {
      reply_ctx(ctx, -EPERM);
      return;
    }

    ctx->multi_object_write_op_ctx = moc = multi_object_write_op_create(soid, ctx->reqid);

    assert(m->get_snapid() == CEPH_NOSNAP);
    if (ctx->is_master()) {
      // client --> master
      dout(20) << __func__ << " multi op master " << soid << dendl;

      moc->is_master = true;
      moc->master = soid;
      moc->master_reqid = m->get_reqid();
      for (auto &it : m->sub_ops)
        moc->sub_objects.insert(make_pair(it.first, make_pair(0, 0)));
      assert(m->sub_ops.size());
    } else {
      // master --> slave
      assert(ctx->is_slave_lock());
      moc->master = hobject_t(m->master,
                              m->get_object_locator().key,
                              m->get_snapid(),
                              0,
                              m->get_object_locator().get_pool(),
                              m->get_object_locator().nspace);
      moc->master_reqid = m->master_reqid;

      dout(20) << __func__ << " multi op slave lock " << moc->master
               << " -> " << soid << dendl;
    }
    moc->locator = m->get_object_locator();
    moc->snapc.seq = m->get_snap_seq();
    moc->snapc.snaps = m->get_snaps();
    moc->mtime = ceph::real_clock::from_ceph_timespec(m->get_mtime());
  } else {
   ctx->multi_object_write_op_ctx = moc;
  }

  assert(moc->state != MultiObjectWriteOpContext::LOADING);
  dout(20) << __func__ << *moc << dendl;

  if (ctx->is_master()) {
    dout(20) << __func__ << " master op" << dendl;

    if (moc->state != MultiObjectWriteOpContext::NEW) {
      moc->waiting.push_back(ctx->op);
      ctx->op->mark_delayed("waiting multi op processing");
      close_op_ctx(ctx);
      return;
    }
    moc->ctx = ctx;
    multi_object_write_op_master_lock_self(moc);
  } else if (ctx->is_slave_lock()) {
    dout(20) << __func__ << " slave lock op" << dendl;

    switch (moc->state)
    {
      case MultiObjectWriteOpContext::NEW:
        moc->state = MultiObjectWriteOpContext::LOCKING;
        ctx->register_on_finish(
          [moc, this](){
            if (moc->destroyed)
              return;

            if (moc->state == MultiObjectWriteOpContext::LOCKING) {
              //lock failed
              requeue_ops(moc->waiting);
              multi_object_write_op_destroy(moc);
            }
          });
        break;
      case MultiObjectWriteOpContext::LOCK:
        reply_ctx(ctx, -EINPROGRESS);
        return;
      default:
        assert(0 == "unexpected case");
    }
    execute_ctx(ctx);
  } else if (ctx->is_slave_commit()) {
    dout(20) << __func__ << " slave commit op" << dendl;

    switch (moc->state)
    {
      case MultiObjectWriteOpContext::LOCK:
        moc->state = MultiObjectWriteOpContext::COMMITTING;
        ctx->register_on_finish(
          [moc, this](){
            if (moc->destroyed)
              return;

            if (moc->state == MultiObjectWriteOpContext::COMMITTING) {
              dout(1) << __func__ << " slave commit failed" << dendl;
              moc->state = MultiObjectWriteOpContext::LOCK;
            }
          });
        break;
      case MultiObjectWriteOpContext::COMMIT:
      case MultiObjectWriteOpContext::UNLOCKING:
        reply_ctx(ctx, -EPERM);
        return;
      default:
        assert(0 == "unexpected case");
    }
    execute_ctx(ctx);
  } else {
    assert(ctx->is_slave_unlock());
    dout(20) << __func__ << " slave unlock op" << dendl;

    switch (moc->state)
    {
      case MultiObjectWriteOpContext::LOCK:
        //pass
        break;
      case MultiObjectWriteOpContext::UNLOCKING:
        reply_ctx(ctx, -EPERM);
        return;
      default:
        assert(0 == "unexpected case");
    }

    moc->ctx = ctx;
    multi_object_write_op_slave_unlock_self(moc);
  }
}

void ReplicatedPG::dead_lock_avoidance(OpRequestRef op, OpContext* ctx,
  MultiObjectWriteOpContextRef moc)
{
  if (ctx->is_slave() && (moc->state == MultiObjectWriteOpContext::LOCKING ||
    moc->state == MultiObjectWriteOpContext::LOCK)) {
    dout(20) << __func__ << " multi op dead lock avoid" << dendl;
    reply_ctx(ctx, -EDEADLK);
  } else {
    dout(20) << __func__ << " waiting for multi op finish" << dendl;
    op->mark_delayed("waiting for multi op finish");
    moc->waiting.push_back(op);
    close_op_ctx(ctx);
  }
}

void ReplicatedPG::cancel_multi_object_write_ops(bool requeue)
{
  dout(20) << __func__ << " requeue: " << requeue << dendl;
  list<OpRequestRef> ls;

  for (auto &it : multi_object_write_ops) {
    MultiObjectWriteOpContextRef &moc = it.second;

    assert(!moc->destroyed);
    moc->destroyed = true;

    for (auto &t : moc->sub_objects)
      if (t.second.second)
        osd->objecter->op_cancel(t.second.second, -ECANCELED);

    if (moc->ctx) {
      ls.push_back(moc->ctx->op);
      close_op_ctx(moc->ctx);
      moc->ctx = nullptr;
    }

    ls.splice(ls.end(), moc->waiting);
  }
  multi_object_write_ops.clear();

  if (requeue)
    requeue_ops(ls);
}

void ReplicatedPG::multi_object_write_op_master_lock_self(MultiObjectWriteOpContextRef moc)
{
  dout(20) << __func__ << *moc << dendl;
  assert(moc->state == MultiObjectWriteOpContext::NEW);
  moc->state = MultiObjectWriteOpContext::LOCKING;

  multi_object_write_op_save(moc,
    [moc, this](){
      if (moc->destroyed)
        return;

      if (g_conf->osd_debug_multi_object_write_operation_master_lock_self_crash) {
        assert(0 == "inject failure");
      }

      dout(20) << __func__ << " master locked" << dendl;
      moc->state = MultiObjectWriteOpContext::LOCK;

      multi_object_write_op_master_lock_slave(moc);
    });
}

void ReplicatedPG::multi_object_write_op_master_lock_slave(MultiObjectWriteOpContextRef moc)
{
  dout(20) << __func__ << *moc << dendl;
  assert(moc->ctx);
  assert(moc->state == MultiObjectWriteOpContext::LOCK);

  MOSDOp *m = static_cast<MOSDOp*>(moc->ctx->op->get_req());
  multi_object_write_op_send(moc,
    [moc, m, this](const object_t &oid, ObjectOperation &op){
       auto it = m->sub_ops.find(oid);
       assert(it != m->sub_ops.end());
       op.dup(it->second);
       op.master = moc->master.oid;
       op.multi_object_write_operation_lock();
    },
    [moc, this](){
      if (moc->destroyed)
        return;

      if (g_conf->osd_debug_multi_object_write_operation_master_lock_slave_crash) {
        assert(0 == "inject failure");
      }

      int r = 0;
      for (auto &p : moc->sub_objects) {
        assert(p.second.first < 0);
        if (p.second.first == -EINPROGRESS)
          continue;
        r = p.second.first;
        break;
      }

      dout(20) << __func__ << " master send lock result: "
               << cpp_strerror(r) << "(" << r << ")" << dendl;

      if (r < 0) {
        reply_ctx(moc->ctx, r);
        moc->ctx = nullptr;

        multi_object_write_op_master_unlock_slave(moc);
      } else {
        multi_object_write_op_master_commit_self(moc);
      }
   });
}

int ReplicatedPG::multi_object_write_op_slave_lock(OpContext *ctx)
{
  MultiObjectWriteOpContextRef moc = ctx->multi_object_write_op_ctx;
  assert(moc);
  assert(moc->state == MultiObjectWriteOpContext::LOCKING);
  assert(!moc->ctx);

  if (!ctx->op_t->empty() || ctx->modify) {
    bufferlist &bl = moc->slave_data_bl;
    ObjectState &obs = ctx->new_obs;
    object_info_t &oi = obs.oi;

    ENCODE_START(1, 1, bl);

    ::encode(ctx->user_modify, bl);
    ::encode(ctx->modify, bl);

    ::encode(ctx->num_read, bl);
    ::encode(ctx->num_write, bl);
    ::encode(ctx->delta_stats, bl);

    ::encode(ctx->modified_ranges, bl);
    ::encode(ctx->mod_desc, bl);

    bool clear_watch = ctx->obs->oi.watchers.size() != oi.watchers.size();
    assert(!clear_watch || oi.watchers.empty());
    ::encode(clear_watch, bl);

    assert(ctx->pending_attrs.empty());

    ::encode(oi.truncate_seq, bl);
    ::encode(oi.truncate_size, bl);
    ::encode(oi.size, bl);

    ::encode(obs.exists, bl);

    bool is_whiteout = oi.is_whiteout();
    ::encode(is_whiteout, bl);

    bool is_data_digest =
      oi.is_data_digest() &&
      (!ctx->obs->oi.is_data_digest() ||
       (oi.data_digest != ctx->obs->oi.data_digest));
    ::encode(is_data_digest, bl);
    if (is_data_digest)
      ::encode(oi.data_digest, bl);

    bool is_omap = oi.is_omap();
    ::encode(is_omap, bl);

    bool is_omap_digest =
      oi.is_omap_digest() &&
      (!ctx->obs->oi.is_omap_digest() ||
       (oi.omap_digest != ctx->obs->oi.omap_digest));
    ::encode(is_omap_digest, bl);
    if (is_omap_digest)
      ::encode(oi.omap_digest, bl);

    ctx->op_t->encode(bl);

    ENCODE_FINISH(bl);
  }

  moc->ctx = ctx;
  multi_object_write_op_save(moc,
    [moc, this](){
      if (moc->destroyed)
        return;

      if (g_conf->osd_debug_multi_object_write_operation_slave_lock_crash) {
        assert(0 == "inject failure");
      }

      dout(20) << __func__ << " slave locked" << dendl;
      assert(moc->state == MultiObjectWriteOpContext::LOCKING);
      moc->state = MultiObjectWriteOpContext::LOCK;

      reply_ctx(moc->ctx, -EINPROGRESS);
      moc->ctx = nullptr;
    });
  return -EINPROGRESS;
}
