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

const librados::NObjectIterator librados::NObjectIterator::__EndObjectIterator(NULL);

librados::NObjectIterator::NObjectIterator(ObjListCtx *ctx_)
{
#if 0
  impl = new NObjectIteratorImpl(ctx_);
#endif
}

librados::NObjectIterator::~NObjectIterator()
{
#if 0
  delete impl;
#endif
}

librados::NObjectIterator::NObjectIterator(const NObjectIterator &rhs)
{
#if 0
  if (rhs.impl == NULL) {
    impl = NULL;
    return;
  }
  impl = new NObjectIteratorImpl();
  *impl = *(rhs.impl);
#endif
}

librados::NObjectIterator& librados::NObjectIterator::operator=(const librados::NObjectIterator &rhs)
{
#if 0
  if (rhs.impl == NULL) {
    delete impl;
    impl = NULL;
    return *this;
  }
  if (impl == NULL)
    impl = new NObjectIteratorImpl();
  *impl = *(rhs.impl);
  return *this;
#endif
}

bool librados::NObjectIterator::operator==(const librados::NObjectIterator& rhs) const
{
#if 0 
  if (impl && rhs.impl) {
    return *impl == *(rhs.impl);
  } else {
    return impl == rhs.impl;
  }
#endif
}

bool librados::NObjectIterator::operator!=(const librados::NObjectIterator& rhs) const
{
#if 0
  return !(*this == rhs);
#endif
}

const librados::ListObject& librados::NObjectIterator::operator*() const {
#if 0
  ceph_assert(impl);
  return *(impl->get_listobjectp());
#endif
}

const librados::ListObject* librados::NObjectIterator::operator->() const {
#if 0
  ceph_assert(impl);
  return impl->get_listobjectp();
#endif
}

librados::NObjectIterator& librados::NObjectIterator::operator++()
{
#if 0
  ceph_assert(impl);
  impl->get_next();
  return *this;
#endif
}

librados::NObjectIterator librados::NObjectIterator::operator++(int)
{
#if 0
  librados::NObjectIterator ret(*this);
  impl->get_next();
  return ret;
#endif
}

uint32_t librados::NObjectIterator::seek(uint32_t pos)
{
#if 0
  ceph_assert(impl);
  return impl->seek(pos);
#endif
}

uint32_t librados::NObjectIterator::seek(const ObjectCursor& cursor)
{
#if 0
  ceph_assert(impl);
  return impl->seek(cursor);
#endif
}

librados::ObjectCursor librados::NObjectIterator::get_cursor()
{
#if 0
  ceph_assert(impl);
  return impl->get_cursor();
#endif
}

void librados::NObjectIterator::set_filter(const bufferlist &bl)
{
#if 0
  impl->set_filter(bl);
#endif
}

void librados::NObjectIterator::get_next()
{
#if 0
  ceph_assert(impl);
  impl->get_next();
#endif
}

uint32_t librados::NObjectIterator::get_pg_hash_position() const
{
#if 0
  ceph_assert(impl);
  return impl->get_pg_hash_position();
#endif
}
