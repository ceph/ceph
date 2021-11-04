#include "LRemIoCtxImpl.h"
#include "LRemClassHandler.h"
#include "LRemRadosClient.h"
#include "LRemWatchNotify.h"
#include "librados/AioCompletionImpl.h"
#include "include/ceph_assert.h"
#include "common/Finisher.h"
#include "common/valgrind.h"
#include "objclass/objclass.h"
#include <functional>
#include <errno.h>

using namespace std;

const librados::NObjectIterator librados::NObjectIterator::__EndObjectIterator(NULL);

librados::NObjectIterator::NObjectIterator(ObjListCtx *ctx_)
{
  impl = new NObjectIteratorImpl(ctx_);
}

librados::NObjectIterator::~NObjectIterator()
{
  delete impl;
  impl = NULL;
}

librados::NObjectIterator::NObjectIterator(const NObjectIterator &rhs)
{
  if (rhs.impl == NULL) {
    impl = NULL;
    return;
  }
  impl = new NObjectIteratorImpl();
  *impl = *(rhs.impl);
}

librados::NObjectIterator& librados::NObjectIterator::operator=(const librados::NObjectIterator &rhs)
{
  if (rhs.impl == NULL) {
    delete impl;
    impl = NULL;
    return *this;
  }
  if (impl == NULL)
    impl = new NObjectIteratorImpl();
  *impl = *(rhs.impl);
  return *this;
}

bool librados::NObjectIterator::operator==(const librados::NObjectIterator& rhs) const
{
  if (impl && rhs.impl) {
    return *impl == *(rhs.impl);
  } else {
    return impl == rhs.impl;
  }
}

bool librados::NObjectIterator::operator!=(const librados::NObjectIterator& rhs) const
{
  return !(*this == rhs);
}

const librados::ListObject& librados::NObjectIterator::operator*() const {
  ceph_assert(impl);
  return *(impl->get_listobjectp());
}

const librados::ListObject* librados::NObjectIterator::operator->() const {
  ceph_assert(impl);
  return impl->get_listobjectp();
}

librados::NObjectIterator& librados::NObjectIterator::operator++()
{
  ceph_assert(impl);
  impl->get_next();
  return *this;
}

librados::NObjectIterator librados::NObjectIterator::operator++(int)
{
  librados::NObjectIterator ret(*this);
  impl->get_next();
  return ret;
}

uint32_t librados::NObjectIterator::seek(uint32_t pos)
{
  ceph_assert(impl);
  return impl->seek(pos);
}

uint32_t librados::NObjectIterator::seek(const ObjectCursor& cursor)
{
  ceph_assert(impl);
  return impl->seek(cursor);
}

librados::ObjectCursor librados::NObjectIterator::get_cursor()
{
  ceph_assert(impl);
  return impl->get_cursor();
}

void librados::NObjectIterator::set_filter(const bufferlist &bl)
{
  impl->set_filter(bl);
}

void librados::NObjectIterator::get_next()
{
  ceph_assert(impl);
  impl->get_next();
}

uint32_t librados::NObjectIterator::get_pg_hash_position() const
{
  ceph_assert(impl);
  return impl->get_pg_hash_position();
}

/* impl */

librados::NObjectIteratorImpl::NObjectIteratorImpl(ObjListCtx *ctx_)
  : ctx(ctx_)
{
}

librados::NObjectIteratorImpl::~NObjectIteratorImpl()
{
  ctx.reset();
}

librados::NObjectIteratorImpl::NObjectIteratorImpl(const NObjectIteratorImpl &rhs)
{
  *this = rhs;
}

librados::NObjectIteratorImpl& librados::NObjectIteratorImpl::operator=(const librados::NObjectIteratorImpl &rhs)
{
  if (&rhs == this)
    return *this;
  if (rhs.ctx.get() == NULL) {
    ctx.reset();
    return *this;
  }
  ctx.reset(new ObjListCtx(*rhs.ctx));
  cur_obj = rhs.cur_obj;
  return *this;
}

bool librados::NObjectIteratorImpl::operator==(const librados::NObjectIteratorImpl& rhs) const {

  if (ctx.get() == NULL) {
    if (rhs.ctx.get() == NULL)
      return true;
    return rhs.ctx->at_end;
  }
  if (rhs.ctx.get() == NULL) {
    // Redundant but same as ObjectIterator version
    if (ctx.get() == NULL)
      return true;
    return ctx->at_end;
  }
  return ctx.get() == rhs.ctx.get();
}

bool librados::NObjectIteratorImpl::operator!=(const librados::NObjectIteratorImpl& rhs) const {
  return !(*this == rhs);
}

const librados::ListObject& librados::NObjectIteratorImpl::operator*() const {
  return cur_obj;
}

const librados::ListObject* librados::NObjectIteratorImpl::operator->() const {
  return &cur_obj;
}

librados::NObjectIteratorImpl& librados::NObjectIteratorImpl::operator++()
{
  get_next();
  return *this;
}

librados::NObjectIteratorImpl librados::NObjectIteratorImpl::operator++(int)
{
  librados::NObjectIteratorImpl ret(*this);
  get_next();
  return ret;
}

uint32_t librados::NObjectIteratorImpl::seek(uint32_t pos)
{
  uint32_t r = rados_nobjects_list_seek(ctx.get(), pos);
  get_next();
  return r;
}

uint32_t librados::NObjectIteratorImpl::seek(const ObjectCursor& cursor)
{
  uint32_t r = rados_nobjects_list_seek_cursor(ctx.get(), (rados_object_list_cursor)cursor.c_cursor);
  get_next();
  return r;
}

librados::ObjectCursor librados::NObjectIteratorImpl::get_cursor()
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)ctx.get();
  return lh->get_cursor();
}

void librados::NObjectIteratorImpl::set_filter(const bufferlist &bl)
{
  ceph_assert(ctx);
  ctx->set_filter(bl);
}

void librados::NObjectIteratorImpl::get_next()
{
  const char *entry, *key, *nspace;
  size_t entry_size, key_size, nspace_size;
  if (ctx->at_end)
    return;
  int ret = rados_nobjects_list_next2(ctx.get(), &entry, &key, &nspace,
                                      &entry_size, &key_size, &nspace_size);
  if (ret == -ENOENT) {
    return;
  }
  else if (ret) {
    throw std::system_error(-ret, std::system_category(),
                            "rados_nobjects_list_next2");
  }

  if (cur_obj.impl == NULL)
    cur_obj.impl = new ListObjectImpl();
  cur_obj.impl->nspace = string{nspace, nspace_size};
  cur_obj.impl->oid = string{entry, entry_size};
  cur_obj.impl->locator = key ? string(key, key_size) : string();
}

uint32_t librados::NObjectIteratorImpl::get_pg_hash_position() const
{
  return 0; // ctx->nlc->get_pg_hash_position();
}



// ListObject
//

librados::ListObject::ListObject() : impl(NULL)
{
}

librados::ListObject::ListObject(librados::ListObjectImpl *i): impl(i)
{
}

librados::ListObject::ListObject(const ListObject& rhs)
{
  if (rhs.impl == NULL) {
    impl = NULL;
    return;
  }
  impl = new ListObjectImpl();
  *impl = *(rhs.impl);
}

const std::string& librados::ListObject::get_nspace() const
{
  return impl->get_nspace();
}

const std::string& librados::ListObject::get_oid() const
{
  return impl->get_oid();
}

const std::string& librados::ListObject::get_locator() const
{
  return impl->get_locator();
}

librados::ListObject& librados::ListObject::operator=(const ListObject& rhs)
{
  if (rhs.impl == NULL) {
    delete impl;
    impl = NULL;
    return *this;
  }
  if (impl == NULL)
    impl = new ListObjectImpl();
  *impl = *(rhs.impl);
  return *this;
}

librados::ListObject::~ListObject()
{
  if (impl)
    delete impl;
  impl = NULL;
}

librados::ObjectCursor::ObjectCursor()
{
  c_cursor = (rados_object_list_cursor)new string;
}

librados::ObjectCursor::ObjectCursor(const librados::ObjectCursor &rhs)
{
  *this = rhs;
}

librados::ObjectCursor::~ObjectCursor()
{
  string *h = (string *)c_cursor;
  delete h;
}

librados::ObjectCursor::ObjectCursor(rados_object_list_cursor c)
{
  if (!c) {
    c_cursor = nullptr;
  } else {
    c_cursor = (rados_object_list_cursor)new string(*(string *)c);
  }
}

librados::ObjectCursor& librados::ObjectCursor::operator=(const librados::ObjectCursor& rhs)
{
  if (rhs.c_cursor != nullptr) {
    string *h = (string *)rhs.c_cursor;
    c_cursor = (rados_object_list_cursor)(new string(*h));
  } else {
    c_cursor = nullptr;
  }
  return *this;
}

bool librados::ObjectCursor::operator<(const librados::ObjectCursor &rhs) const
{
  const string lhs_hobj = (c_cursor == nullptr) ? string() : *((string *)c_cursor);
  const string rhs_hobj = (rhs.c_cursor == nullptr) ? string() : *((string *)(rhs.c_cursor));
  return lhs_hobj < rhs_hobj;
}

bool librados::ObjectCursor::operator==(const librados::ObjectCursor &rhs) const
{
  const string lhs_hobj = (c_cursor == nullptr) ? string() : *((string *)c_cursor);
  const string rhs_hobj = (rhs.c_cursor == nullptr) ? string() : *((string *)(rhs.c_cursor));
  return lhs_hobj == rhs_hobj;
}

string librados::ObjectCursor::to_str() const
{
  return *(string *)c_cursor;
}

bool librados::ObjectCursor::from_str(const string& s)
{
  if (s.empty()) {
    *(string *)c_cursor = string();
    return true;
  }
  
  *(string *)c_cursor = s;
  return true;
}

