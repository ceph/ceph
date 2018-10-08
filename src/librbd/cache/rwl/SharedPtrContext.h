// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SHARED_PTR_CTX_H
#define CEPH_SHARED_PTR_CTX_H

#include <memory>
#include "include/Context.h"
#include "common/WorkQueue.h"

/*
 * A Context variant that can provide shared_ptrs to itself. Calling
 * complete() triggers finish() in the derived class as usual, but the
 * SharedPtrContext isn't destroyed until the last shared_ptr to it
 * goes away. The derived class must of course provide synchronization
 * between its finish() and whatever other methods are called after
 * that via shared_ptrs.
 *
 * We do this by storing a shared_ptr to the SharedPtrContext in
 * itself during creation, and setting it to nullptr in
 * complete(). The create() static method is provided to do this all
 * at once. Your class may need to define create() as a wrapper for 
 * this create method.
 *
 * A bare pointer to these behaves like any other Context*. Lambdas
 * that call other methods asynchronously on classes derived from this
 * should do that via a sharted_ptr (obtained from
 * T->shared_from_this()).
 *
 * See ExampleSPCtx for how to produce a shared_ptr to a derived
 * class from itself.
 */
class SharedPtrContext;

template <typename T>
inline std::shared_ptr<T>
shared_from_base(std::enable_shared_from_this<T>* base)
{
  return base->shared_from_this();
}

template <typename T>
inline std::shared_ptr<const T>
shared_from_base(std::enable_shared_from_this<T> const* base)
{
  return base->shared_from_this();
}

template <typename T>
inline std::shared_ptr<T>
shared_from(T* derived)
{
  return std::static_pointer_cast<T>(shared_from_base(derived));
}

class SharedPtrContext: public Context, public std::enable_shared_from_this<SharedPtrContext>
{
private:
  std::shared_ptr<SharedPtrContext> m_self_reference = nullptr;
  SharedPtrContext(const SharedPtrContext& other);
  const SharedPtrContext& operator=(const SharedPtrContext& other);
  void ref_self() {
    assert(!m_self_reference);
    m_self_reference = shared_from_this();
  }

 protected:
  virtual void finish(int r) {};

 public:
  SharedPtrContext() {}
  virtual ~SharedPtrContext() {}       // we want a virtual destructor!!!
  template <typename T, typename U>
  /* Factory for classes derived from this. */
  static std::shared_ptr<T> create(U&& arg)
  {
    std::shared_ptr<T> sp_ctx = std::make_shared<T>(std::forward<U>(arg));
    sp_ctx->ref_self();
    return sp_ctx;
  }
  template <typename T, typename... U>
  static std::shared_ptr<T> create(U&&... arg)
  {
    std::shared_ptr<T> sp_ctx = std::make_shared<T>(std::forward<U>(arg)...);
    sp_ctx->ref_self();
    return sp_ctx;
  }
  virtual void complete(int r) {
    finish(r);
    assert(m_self_reference);
    m_self_reference = nullptr;
  }
  virtual bool sync_complete(int r) {
    if (sync_finish(r)) {
      assert(m_self_reference);
      m_self_reference = nullptr;
    }
    return false;
  }
};

class ExampleSPCtx : public SharedPtrContext
{
  int m_foo;
  void finish(int r) {
    m_foo += r;
  }
public:
  ExampleSPCtx(int foo) : m_foo(foo) {}
  ~ExampleSPCtx() {}
  /* Add this to your class if template argument deduction fails */
  template <typename... U>
  static inline std::shared_ptr<ExampleSPCtx> create(U&&... arg)
  {
    return SharedPtrContext::create<ExampleSPCtx>(std::forward<U>(arg)...);
  }
  /* Include this when deriving from SharedPtrContext to produce
   * shared_ptrs to the derived class. */
  auto shared_from_this() {
    return shared_from(this);
  }
};

#endif
