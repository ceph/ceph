// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_INTERFACE_H
#define CEPH_ERASURE_CODE_INTERFACE_H

/*! @file ErasureCodeInterface.h
    @brief Interface provided by erasure code plugins

    The erasure coded pools rely on plugins implementing
    **ErasureCodeInterface** to encode and decode content. All codes
    are systematic (i.e. the data is not mangled and can be
    reconstructed by concatenating chunks ).
    
    All methods returns **0** on success and a negative value on
    error. If the value returned on error is not explained in
    **ErasureCodeInterface**, the sources or the documentation of the
    interface implementer must be read to figure out what it means. It
    is recommended that each error code matches an *errno* value that
    relates to the cause of the error.

    Assuming the interface implementer provides three data chunks ( K
    = 3 ) and two coding chunks ( M = 2 ), a buffer can be encoded as
    follows:
    
    ~~~~~~~~~~~~~~~~{.c}
    set<int> want_to_encode(0, 1, 2, // data chunks
                            3, 4     // coding chunks
                           );
    bufferlist in = "ABCDEF";
    map<int, bufferlist> encoded
    encode(want_to_encode, in, &encoded);
    encoded[0] == "AB" // data chunk 0
    encoded[1] == "CD" // data chunk 1
    encoded[2] == "EF" // data chunk 2
    encoded[3]         // coding chunk 0
    encoded[4]         // coding chunk 1
    ~~~~~~~~~~~~~~~~

    If encoded[2] ( which contains **EF** ) is missing and accessing
    encoded[3] ( the first coding chunk ) is more expensive than
    accessing encoded[4] ( the second coding chunk ), the
    **minimum_to_decode_with_cost** method can be called as follows:

    ~~~~~~~~~~~~~~~~{.c}
    set<int> want_to_read(2); // want the chunk containing "EF"
    map<int,int> available(
          0 => 1,  // data chunk 0 : available and costs 1
          1 => 1,  // data chunk 1 : available and costs 1
          3 => 9,  // coding chunk 1 : available and costs 9
          4 => 1,  // coding chunk 2 : available and costs 1
    );
    set<int> minimum;
    minimum_to_decode_with_cost(want_to_read,
                                available,
                                &minimum);
    minimum == set<int>(0, 1, 4);
    ~~~~~~~~~~~~~~~~
    
    It sets **minimum** with three chunks to reconstruct the desired
    data chunk and will pick the second coding chunk ( 4 ) because it
    is less expensive ( 1 < 9 ) to retrieve than the first coding
    chunk ( 3 ). The caller is responsible for retrieving the chunks
    and call **decode** to reconstruct the second data chunk content.
    
    ~~~~~~~~~~~~~~~~{.c}
    map<int,bufferlist> chunks;
    for i in minimum.keys():
      chunks[i] = fetch_chunk(i); // get chunk from storage
    map<int, bufferlist> decoded;
    decode(want_to_read, chunks, &decoded);
    decoded[2] == "EF"
    ~~~~~~~~~~~~~~~~

 */ 

#include <map>
#include <set>
#include <tr1/memory>
#include "include/buffer.h"

using namespace std;

namespace ceph {

  class ErasureCodeInterface {
  public:
    virtual ~ErasureCodeInterface() {}

    /**
     * Compute the smallest subset of **available** chunks that needs
     * to be retrieved in order to successfully decode
     * **want_to_read** chunks.
     *
     * It is strictly equivalent to calling
     * **minimum_to_decode_with_cost** where each **available** chunk
     * has the same cost.
     *
     * @see minimum_to_decode_with_cost 
     *
     * @param [in] want_to_read chunk indexes to be decoded
     * @param [in] available chunk indexes containing valid data
     * @param [out] minimum chunk indexes to retrieve for decode
     * @return **0** on success or a negative errno on error.
     */
    virtual int minimum_to_decode(const set<int> &want_to_read,
                                  const set<int> &available,
                                  set<int> *minimum) = 0;

    /**
     * Compute the smallest subset of **available** chunks that needs
     * to be retrieved in order to successfully decode
     * **want_to_read** chunks. If there are more than one possible
     * subset, select the subset that contains the chunks with the
     * lowest cost.
     *
     * The **available** parameter maps chunk indexes to their
     * retrieval cost. The higher the cost value, the more costly it
     * is to retrieve the chunk content. 
     *
     * Returns -EIO if there are not enough chunk indexes in
     * **available** to decode **want_to_read**.
     *
     * Returns 0 on success.
     *
     * The **minimum** argument must be a pointer to an empty set.
     *
     * @param [in] want_to_read chunk indexes to be decoded
     * @param [in] available map chunk indexes containing valid data 
     *             to their retrieval cost
     * @param [out] minimum chunk indexes to retrieve for decode
     * @return **0** on success or a negative errno on error.
     */
    virtual int minimum_to_decode_with_cost(const set<int> &want_to_read,
                                            const map<int, int> &available,
                                            set<int> *minimum) = 0;

    /**
     * Encode the content of **in** and store the result in
     * **encoded**. The **encoded** map contains at least all
     * chunk indexes found in the **want_to_encode** set. 
     *
     * The **encoded** map is expected to be a pointer to an empty
     * map.
     *
     * The **encoded** map may contain more chunks than required by
     * **want_to_encode** and the caller is expected to permanently
     * store all of them, not just the chunks from **want_to_encode**.
     *
     * Returns 0 on success.
     *
     * @param [in] want_to_encode chunk indexes to be encoded
     * @param [in] in data to be encoded
     * @param [out] encoded map chunk indexes to chunk data
     * @return **0** on success or a negative errno on error.
     */
    virtual int encode(const set<int> &want_to_encode,
                       const bufferlist &in,
                       map<int, bufferlist> *encoded) = 0;

    /**
     * Decode the **chunks** and store at least **want_to_read** chunks
     * in **decoded**. 
     *
     * There must be enough **chunks** ( as returned by
     * **minimum_to_decode** or **minimum_to_decode_with_cost** ) to
     * perform a successfull decoding of all chunks found in
     * **want_to_read**.
     *
     * The **decoded** map is expected to be a pointer to an empty
     * map.
     *
     * The **decoded** map may contain more chunks than required by
     * **want_to_read** and they can safely be used by the caller.
     *
     * If a chunk is listed in **want_to_read** and there is
     * corresponding **bufferlist** in **chunks**, it will be copied
     * verbatim into **decoded**. If not it will be reconstructed from
     * the existing chunks.
     *
     * Returns 0 on success.
     *
     * @param [in] want_to_read chunk indexes to be decoded
     * @param [in] chunks map chunk indexes to chunk data
     * @param [out] decoded map chunk indexes to chunk data
     * @return **0** on success or a negative errno on error.
     */
    virtual int decode(const set<int> &want_to_read,
                       const map<int, bufferlist> &chunks,
                       map<int, bufferlist> *decoded) = 0;
  };

  typedef std::tr1::shared_ptr<ErasureCodeInterface> ErasureCodeInterfaceRef;

}

#endif
