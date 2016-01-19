#ifndef __LIBRADOSSTRIPER_HPP
#define __LIBRADOSSTRIPER_HPP

#include <string.h>
#include <string>
#include <map>
#include "../rados/buffer.h"
#include "../rados/librados.hpp"

#include "libradosstriper.h"

namespace libradosstriper
{
  struct RadosStriperImpl;
  struct MultiAioCompletionImpl;

  /*
   * Completion object for multiple asynchronous IO
   * It allows to internally handle several "requests"
   */
  struct MultiAioCompletion {
    MultiAioCompletion(MultiAioCompletionImpl *pc_) : pc(pc_) {}
    ~MultiAioCompletion();
    int set_complete_callback(void *cb_arg, librados::callback_t cb);
    int set_safe_callback(void *cb_arg, librados::callback_t cb);
    void wait_for_complete();
    void wait_for_safe();
    void wait_for_complete_and_cb();
    void wait_for_safe_and_cb();
    bool is_complete();
    bool is_safe();
    bool is_complete_and_cb();
    bool is_safe_and_cb();
    int get_return_value();
    void release();
    MultiAioCompletionImpl *pc;
  };

  /* RadosStriper : This class allows to perform read/writes on striped objects
   *
   * Typical use (error checking omitted):
   *
   * RadosStriper rs;
   * RadosStriper.striper_create("my_cluster", rs);
   * bufferlist bl;
   * ... put data in bl ...
   * rs.write(object_name, bl, len, offset);
   * bufferlist bl2;
   * rs.read(object_name, &bl2, len, offset);
   * ...
   */
  class RadosStriper
  {
  public:

    /*
     * constructor
     */
    RadosStriper();

    /*
     * builds the C counter part of a RadosStriper
     */
    static void to_rados_striper_t(RadosStriper &striper,
                                   rados_striper_t *s);

    /*
     * copy constructor
     */
    RadosStriper(const RadosStriper& rs);

    /*
     * operator=
     */
    RadosStriper& operator=(const RadosStriper& rs);

    /*
     * destructor
     * Internally calling close() if an object is currently opened
     */
    ~RadosStriper();

    /*
     * create method
     */
    static int striper_create(librados::IoCtx& ioctx,
                              RadosStriper *striper);

    /*
     * set object layout's stripe unit
     * This layout will be used when new objects are created (by writing to them)
     * Already existing objects will be opened with their own layout.
     */
    int set_object_layout_stripe_unit(unsigned int stripe_unit);

    /*
     * set object layout's stripe count
     * This layout will be used when new objects are created (by writing to them)
     * Already existing objects will be opened with their own layout.
     */
    int set_object_layout_stripe_count(unsigned int stripe_count);

    /*
     * set object layout's object size
     * This layout will be used when new objects are created (by writing to them)
     * Already existing objects will be opened with their own layout.
     */
    int set_object_layout_object_size(unsigned int object_size);

    /**
     * Get the value of an extended attribute on a striped object
     */
    int getxattr(const std::string& oid, const char *name, ceph::bufferlist& bl);

    /**
     * Set the value of an extended attribute on a striped object
     */
    int setxattr(const std::string& oid, const char *name, ceph::bufferlist& bl);

    /**
     * Delete an extended attribute from a striped object
     */
    int rmxattr(const std::string& oid, const char *name);

    /**
     * Start iterating over xattrs on a striped object.
     */
    int getxattrs(const std::string& oid,
                  std::map<std::string, ceph::bufferlist>& attrset); 
    
    /**
     * synchronously write to the striped object at the specified offset.
     * NOTE: this call steals the contents of @param bl.
     */
    int write(const std::string& soid, const ceph::bufferlist& bl, size_t len, uint64_t off);

    /**
     * synchronously fill the striped object with the specified data
     * NOTE: this call steals the contents of @param bl.
     */
    int write_full(const std::string& soid, const ceph::bufferlist& bl);

    /**
     * synchronously append data to the striped object
     * NOTE: this call steals the contents of @p bl.
     */
    int append(const std::string& soid, const ceph::bufferlist& bl, size_t len);

    /**
     * asynchronously write to the striped object at the specified offset.
     * NOTE: this call steals the contents of @p bl.
     */
    int aio_write(const std::string& soid, librados::AioCompletion *c, const ceph::bufferlist& bl, size_t len, uint64_t off);

    /**
     * asynchronously fill the striped object with the specified data
     * NOTE: this call steals the contents of @p bl.
     */
    int aio_write_full(const std::string& soid, librados::AioCompletion *c, const ceph::bufferlist& bl);

    /**
     * asynchronously append data to the striped object
     * NOTE: this call steals the contents of @p bl.
     */
    int aio_append(const std::string& soid, librados::AioCompletion *c, const ceph::bufferlist& bl, size_t len);

    /**
     * synchronously read from the striped object at the specified offset.
     */
    int read(const std::string& soid, ceph::bufferlist* pbl, size_t len, uint64_t off);

    /**
     * asynchronously read from the striped object at the specified offset.
     */
    int aio_read(const std::string& soid, librados::AioCompletion *c, ceph::bufferlist *pbl, size_t len, uint64_t off);

    /**
     * synchronously get striped object stats (size/mtime)
     */
    int stat(const std::string& soid, uint64_t *psize, time_t *pmtime);

    /**
     * deletes a striped object.
     * There is no atomicity of the deletion and the striped
     * object may be left incomplete if an error is returned (metadata
     * all present, but some stripes missing)
     * However, there is a atomicity of the metadata deletion and
     * the deletion can not happen if any I/O is ongoing (it
     * will return EBUSY). Identically, no I/O will be able to start
     * during deletion (same EBUSY return code)
     */
    int remove(const std::string& soid);
    int remove(const std::string& soid, int flags);
    /**
     * Resizes a striped object
     * the truncation can not happen if any I/O is ongoing (it
     * will return EBUSY). Identically, no I/O will be able to start
     * during truncation (same EBUSY return code)
     */
    int trunc(const std::string& oid, uint64_t size);

    /**
     * Wait for all currently pending aio writes to be safe.
     *
     * @returns 0 on success, negative error code on failure
     */
    int aio_flush();

    /**
     * creation of multi aio completion objects
     */
    static MultiAioCompletion *multi_aio_create_completion();
    static MultiAioCompletion *multi_aio_create_completion(void *cb_arg,
                                                           librados::callback_t cb_complete,
                                                           librados::callback_t cb_safe);

  private:
    RadosStriperImpl *rados_striper_impl;

  };

}

#endif
