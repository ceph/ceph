// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
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
    
    Methods returning an **int** return **0** on success and a
    negative value on error. If the value returned on error is not
    explained in **ErasureCodeInterface**, the sources or the
    documentation of the interface implementer (i.e. the plugin ) must
    be read to figure out what it means. It is recommended that each
    error code matches an *errno* value that relates to the cause of
    the error.

    If an object is small enough, the caller can process it with
    one call to the **encode** or **decode** method.

	+---------------- coded object O -------------------------+
	|+----------------+ +----------------+ +----------------+ |
	||    chunk  0    | |    chunk  1    | |    chunk  2    | |
	||    [0,N)       | |    [N,2N)      | |    [2N,3N)     | |
	|+----------------+ +----------------+ +----------------+ |
	+------^--------------------------------------------------+
	       |
   chunk B / C | offset B % C ( where C is the chunk size )
	       |
	 +-----^---- raw object O ----+------+
	 |     B     [0,X)            | pad  |
	 +----------------------------+------+

    The object size is paded so that each chunks are of the same size.
    In the example above, if the actual object size was X, then it
    will be padded to 2N >= X assuming there are two data chunks (0
    and 1) and one coding chunk (2).

    For chunks of size C, byte B of the object is found in chunk number
    B / C at offset B % C.

    If an object is too large to be encoded in memory, the caller
    should divide it in smaller units named **stripes**.

	+---------------------- object O -------------------------+
	|+----------------+ +----------------+ +----------------+ |
 stripe ||    chunk  0    | |    chunk  1    | |    chunk  2    | |
   0    ||    [0,N)       | |    [N,2N)      | |    [2N,3N)     | |
	|+----------------+ +----------------+ +----------------+ |
	|+----------------+ +----------------+ +----------------+ |
 stripe ||    chunk  0    | |    chunk  1    | |    chunk  2    | |
   1    ||    [X,M)       | |   [X+M,X+2M)   | |   [X+2M,X+3M)  | |
	||                | |                | |                | |
	|+----------------+ +----------------+ +----------------+ |
	|                         ...                             |
	+---------------------------------------------------------+

    The interface does not concern itself with stripes nor does it
    impose constraints on the size of each stripe. Variable names in
    the interface always use **object** and never use **stripe**. 

    Assuming the interface implementer provides three data chunks ( K
    = 3 ) and two coding chunks ( M = 2 ), a buffer could be encoded as
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

    The **minimum_to_decode_with_cost** method can be used to minimize
    the cost of fetching the chunks necessary to retrieve a given
    content. For instance, if encoded[2] (contained **EF**) is missing
    and accessing encoded[3] (the first coding chunk) is more
    expensive than accessing encoded[4] (the second coding chunk),
    **minimum_to_decode_with_cost** is expected to chose the first
    coding chunk.

    ~~~~~~~~~~~~~~~~{.c}
    set<int> want_to_read(2); // want the chunk containing "EF"
    map<int,int> available(
          0 => 1,  // data chunk 0 : available and costs 1
          1 => 1,  // data chunk 1 : available and costs 1
                   // data chunk 2 : missing
          3 => 9,  // coding chunk 1 : available and costs 9
          4 => 1,  // coding chunk 2 : available and costs 1
    );
    set<int> minimum;
    minimum_to_decode_with_cost(want_to_read,
                                available,
                                &minimum);
    minimum == set<int>(0, 1, 4); // NOT set<int>(0, 1, 3);
    ~~~~~~~~~~~~~~~~
    
    It sets **minimum** with three chunks to reconstruct the desired
    data chunk and will pick the second coding chunk ( 4 ) because it
    is less expensive ( 1 < 9 ) to retrieve than the first coding
    chunk ( 3 ). The caller is responsible for retrieving the chunks
    and call **decode** to reconstruct the second data chunk.
    
    ~~~~~~~~~~~~~~~~{.c}
    map<int,bufferlist> chunks;
    for i in minimum.keys():
      chunks[i] = fetch_chunk(i); // get chunk from storage
    map<int, bufferlist> decoded;
    decode(want_to_read, chunks, &decoded);
    decoded[2] == "EF"
    ~~~~~~~~~~~~~~~~

    The semantic of the cost value is defined by the caller and must
    be known to the implementer. For instance, it may be more
    expensive to retrieve two chunks with cost 1 + 9 = 10 than two
    chunks with cost 6 + 6 = 12. 
 */ 

#include <map>
#include <set>
#include <vector>
#include <ostream>
#include <memory>
#include <string>
#include "include/buffer_fwd.h"
#include "osd/osd_types.h"

#define IGNORE_DEPRECATED \
  _Pragma("GCC diagnostic push") \
  _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\" ") \
  _Pragma("clang diagnostic push") \
  _Pragma("clang diagnostic ignored \"-Wdeprecated-declarations\"")

#define END_IGNORE_DEPRECATED \
  _Pragma("clang pop") \
  _Pragma("GCC pop")


class CrushWrapper;

namespace ceph {

  typedef std::map<std::string,std::string> ErasureCodeProfile;

  inline std::ostream& operator<<(std::ostream& out, const ErasureCodeProfile& profile) {
    out << "{";
    for (ErasureCodeProfile::const_iterator it = profile.begin();
	 it != profile.end();
	 ++it) {
      if (it != profile.begin()) out << ",";
      out << it->first << "=" << it->second;
    }
    out << "}";
    return out;
  }


  class ErasureCodeInterface {
  public:
    virtual ~ErasureCodeInterface() {}

    /**
     * Initialize the instance according to the content of
     * **profile**. The **ss** stream is set with debug messages or
     * error messages, the content of which depend on the
     * implementation.
     *
     * Return 0 on success or a negative errno on error. When
     * returning on error, the implementation is expected to
     * provide a human readable explanation in **ss**.
     *
     * @param [in] profile a key/value map
     * @param [out] ss contains informative messages when an error occurs
     * @return 0 on success or a negative errno on error.
     */
    virtual int init(ErasureCodeProfile &profile, std::ostream *ss) = 0;

    /**
     * Return the profile that was used to initialize the instance
     * with the **init** method.
     *
     * @return the profile in use by the instance
     */
    virtual const ErasureCodeProfile &get_profile() const = 0;

    /**
     * Create a new rule in **crush** under the name **name**,
     * unless it already exists.
     *
     * Return the rule number that was created on success. If a
     * rule **name** already exists, return -EEXISTS, otherwise
     * return a negative value indicating an error with a semantic
     * defined by the implementation.
     *
     * @param [in] name of the rule to create
     * @param [in] crush crushmap in which the rule is created
     * @param [out] ss contains informative messages when an error occurs
     * @return a rule on success or a negative errno on error.
     */
    virtual int create_rule(const std::string &name,
			    CrushWrapper &crush,
			    std::ostream *ss) const = 0;

    /**
     * Return the number of chunks created by a call to the **encode**
     * method.
     *
     * In the simplest case it can be K + M, i.e. the number
     * of data chunks (K) plus the number of parity chunks
     * (M). However, if the implementation provides local parity there
     * could be an additional overhead.
     *
     * @return the number of chunks created by encode()
     */
    virtual unsigned int get_chunk_count() const = 0;

    /**
     * Return the number of data chunks created by a call to the
     * **encode** method. The data chunks contain the buffer provided
     * to **encode**, verbatim, with padding at the end of the last
     * chunk.
     *
     * @return the number of data chunks created by encode()
     */
    virtual unsigned int get_data_chunk_count() const = 0;

    /**
     * Return the number of coding chunks created by a call to the
     * **encode** method. The coding chunks are used to recover from
     * the loss of one or more chunks. If there is one coding chunk,
     * it is possible to recover from the loss of exactly one
     * chunk. If there are two coding chunks, it is possible to
     * recover from the loss of at most two chunks, etc.
     *
     * @return the number of coding chunks created by encode()
     */
    virtual unsigned int get_coding_chunk_count() const = 0;

    /**
     * Return the number of sub chunks chunks created by a call to the
     * **encode** method. Each chunk can be viewed as union of sub-chunks
     * For the case of array codes, the sub-chunk count > 1, where as the
     * scalar codes have sub-chunk count = 1.
     *
     * @return the number of sub-chunks per chunk created by encode()
     */
    virtual int get_sub_chunk_count() = 0;

    /**
     * Return the size (in bytes) of a single chunk created by a call
     * to the **decode** method. The returned size multiplied by
     * **get_chunk_count()** is greater or equal to **stripe_width**.
     *
     * If the object size is properly aligned, the chunk size is
     * **stripe_width / get_chunk_count()**. However, if
     * **stripe_width** is not a multiple of **get_chunk_count** or if
     * the implementation imposes additional alignment constraints,
     * the chunk size may be larger.
     *
     * The byte found at offset **B** of the original object is mapped
     * to chunk **B / get_chunk_size()** at offset **B % get_chunk_size()**.
     *
     * @param [in] stripe_width the number of bytes of the object to **encode()**
     * @return the size (in bytes) of a single chunk created by **encode()**
     */
    virtual unsigned int get_chunk_size(unsigned int stripe_width) const = 0;

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
     * @param [out] minimum chunk indexes and corresponding 
     *              subchunk index offsets, count.
     * @return **0** on success or a negative errno on error.
     */
    virtual int minimum_to_decode(const shard_id_set &want_to_read,
                          const shard_id_set &available,
                          shard_id_set &minimum_set,
                          mini_flat_map<shard_id_t, std::vector<std::pair<int, int>>> *minimum_sub_chunks) = 0;

    // Interface for legacy EC.
    [[deprecated]]
    virtual int minimum_to_decode(const std::set<int> &want_to_read,
                                  const std::set<int> &available,
                                  std::map<int, std::vector<std::pair<int, int>>> *minimum) = 0;

    /**
     * Compute the smallest subset of **available** chunks that needs
     * to be retrieved in order to successfully decode
     * **want_to_read** chunks. If there are more than one possible
     * subset, select the subset that minimizes the overall retrieval
     * cost.
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
     * @param [out] minimum chunk indexes to retrieve 
     * @return **0** on success or a negative errno on error.
     */
    virtual int minimum_to_decode_with_cost(const shard_id_set &want_to_read,
                                            const shard_id_map<int> &available,
                                            shard_id_set *minimum) = 0;

    [[deprecated]]
    virtual int minimum_to_decode_with_cost(const std::set<int> &want_to_read,
                                            const std::map<int, int> &available,
                                            std::set<int> *minimum) = 0;

    /**
     * Return the minimum number of bytes that the plugin and technique
     * support for partial writes. This is the minimum size of update
     * to coding chunks that the particular technique supports.
     *
     * @return minimum number of bytes.
     */
    virtual size_t get_minimum_granularity() = 0;

    /**
     * Note: The encode function is used for the older EC code path
     * that is used when EC optimizations are turned off. EC optimizations
     * are turned off for new pools by default.
     *
     * Encode the content of **in** and store the result in
     * **encoded**. All buffers pointed to by **encoded** have the
     * same size. The **encoded** map contains at least all chunk
     * indexes found in the **want_to_encode** set.
     *
     * The **encoded** map is expected to be a pointer to an empty
     * map.
     *
     * Assuming the **in** parameter is **length** bytes long, 
     * the concatenation of the first **length** bytes of the
     * **encoded** buffers is equal to the content of the **in**
     * parameter.
     *
     * The **encoded** map may contain more chunks than required by
     * **want_to_encode** and the caller is expected to permanently
     * store all of them, not just the chunks listed in
     * **want_to_encode**.
     *
     * The **encoded** map may contain pointers to data stored in
     * the **in** parameter. If the caller modifies the content of
     * **in** after calling the encode method, it may have a side
     * effect on the content of **encoded**. 
     *
     * The **encoded** map may contain pointers to buffers allocated
     * by the encode method. They will be freed when **encoded** is
     * freed. The allocation method is not specified.
     *
     * Returns 0 on success.
     *
     * @param [in] want_to_encode chunk indexes to be encoded
     * @param [in] in data to be encoded
     * @param [out] encoded map chunk indexes to chunk data
     * @return **0** on success or a negative errno on error.
     */
    virtual int encode(const shard_id_set &want_to_encode,
                       const bufferlist &in,
                       shard_id_map<bufferlist> *encoded) = 0;
    [[deprecated]]
     virtual int encode(const std::set<int> &want_to_encode,
                        const bufferlist &in,
                        std::map<int, bufferlist> *encoded) = 0;

    [[deprecated]]
    virtual int encode_chunks(const std::set<int> &want_to_encode,
                              std::map<int, bufferlist> *encoded) = 0;

    /**
     * Note: The encode_chunks function is used by the older EC code path
     * that is used when EC optimizations are turned off. It is also used
     * when EC optimizations are turned on.
     *
     * Encode the content of **in** and store the result in
     * **out**. All buffers pointed to by **in** and **out** have the
     * same size.
     *
     * The data chunks to be encoded are provided in the in map, these buffers
     * are considered to be immutable (neither the bufferptr or the contents
     * of the buffer may be changed). Some of these bufferptrs may be a special
     * bufferptr representing a buffer of zeros. There is no way to represent
     * a buffer for a chunk that consists of a mixture of data and zeros,
     * the caller is expected to make multiple calls to encode_chunks using smaller
     * buffers if this optimzation is worthwhile. The bufferptrs are expected to
     * have suitable alignment (page alignment) and are a single contiguous
     * range of memory. The caller is likely to have a bufferlist per chunk
     * and may either need to make multiple calls to encode_chunks or use
     * rebuild_and_align to create a single contiguous buffer for each chunk.
     *
     * The coding parity chunk bufferptrs are allocated by the caller and
     * populated in the out map. These bufferptrs are expected to be written to
     * by the erasure code plugin. Again the bufferptrs are expected to have
     * suitable alignment and are a single contiguous range of memory.
     * The erasure code plugin may replace one or more of these bufferptrs
     * with a special bufferptr representing a buffer of zeros.
     *
     * Returns 0 on success.
     *
     * @param [in] in map of data shards to be encoded
     * @param [out] out map of empty buffers for parity to be written to
     * @return **0** on success or a negative errno on error.
     */
    virtual int encode_chunks(const shard_id_map<bufferptr> &in,
                              shard_id_map<bufferptr> &out) = 0;

    /**
     * Calculate the delta between the old_data and new_data buffers using xor,
     * (or plugin-specific implementation) and returns the result in the
     * delta_maybe_in_place buffer.
     *
     * Assumes old_data, new_data and delta_maybe_in_place are all buffers of
     * the same length.
     *
     * Optionally, the delta_maybe_in_place and old_data parameters can be the
     * same buffer. For some plugins making these the same buffer is slightly
     * faster, as it avoids a memcpy. Reduced allocations in the caller may
     * also provide a performance advantage.
     *
     * @param [in] old_data first buffer to xor
     * @param [in] new_data second buffer to xor
     * @delta_maybe_in_place [out] delta buffer to write the delta of
     *                       old_data and new_data. This can optionally be a
     *                       pointer to old_data.
     */
    virtual void encode_delta(const bufferptr &old_data,
                              const bufferptr &new_data,
                              bufferptr *delta_maybe_in_place) = 0;

    /**
     * Applies one or more deltas to one or more coding
     * chunks.
     *
     * Assumes all buffers in the in and out maps are the same length.
     *
     * The in map should contain deltas of data chunks to be applied to
     * the coding chunks. The delta for a specific data chunk must have
     * the correct integer key in the map. e.g. if k=2 m=2 and a delta for k[1] 
     * is being applied, then the delta should have key 1 in the in map.
     *
     * The in map should also contain the coding chunks that the delta will
     * be applied to. The coding chunks must also have the correct integer key in the
     * map. e.g. if k=2 m=2 and the delta for k[1] is to be applied to m[1], then
     * the coding chunk should have key 3 in the in map.
     *
     * If a coding buffer is present in the in map, then it must also be present in the 
     * out map with the same key.
     *
     *
     * @param [in] old_data first buffer to xor
     * @param [in] new_data second buffer to xor
     * @param [out] delta buffer containing the delta of old_data and new_data
     */
    virtual void apply_delta(const shard_id_map<bufferptr> &in,
                             shard_id_map<bufferptr> &out) = 0;

    /**
     * N.B This function is not used when EC optimizations are
     * turned on for the pool.
     *
     * Decode the **chunks** and store at least **want_to_read**
     * chunks in **decoded**.
     *
     * The **decoded** map must be a pointer to an empty map.
     *
     * There must be enough **chunks** ( as returned by
     * **minimum_to_decode** or **minimum_to_decode_with_cost** ) to
     * perform a successful decoding of all chunks listed in
     * **want_to_read**.
     *
     * All buffers pointed by **in** must have the same size.
     *
     * On success, the **decoded** map may contain more chunks than
     * required by **want_to_read** and they can safely be used by the
     * caller.
     *
     * If a chunk is listed in **want_to_read** and there is a
     * corresponding **bufferlist** in **chunks**, it will be
     * referenced in **decoded**. If not it will be reconstructed from
     * the existing chunks. 
     *
     * Because **decoded** may contain pointers to data found in
     * **chunks**, modifying the content of **chunks** after calling
     * decode may have a side effect on the content of **decoded**.
     *
     * Returns 0 on success.
     *
     * @param [in] want_to_read chunk indexes to be decoded
     * @param [in] chunks map chunk indexes to chunk data
     * @param [out] decoded map chunk indexes to chunk data
     * @param [in] chunk_size chunk size
     * @return **0** on success or a negative errno on error.
     */
    virtual int decode(const shard_id_set &want_to_read,
                       const shard_id_map<bufferlist> &chunks,
                       shard_id_map<bufferlist> *decoded, int chunk_size) = 0;
    [[deprecated]]
    virtual int decode(const std::set<int> &want_to_read,
                       const std::map<int, bufferlist> &chunks,
                       std::map<int, bufferlist> *decoded, int chunk_size) = 0;

    /**
     * Decode the **in** map and store at least **want_to_read**
     * shards in the **out** map.
     *
     * There must be enough shards in the **in** map( as returned by
     * **minimum_to_decode** or **minimum_to_decode_with_cost** ) to
     * perform a successful decoding of all shards listed in
     * **want_to_read**.
     *
     * All buffers pointed to by **in** must have the same size.
     * **out** must contain empty buffers that are the same size as the
     * **in*** buffers.
     *
     * On success, the **out** map may contain more shards than
     * required by **want_to_read** and they can safely be used by the
     * caller.
     *
     * Returns 0 on success.
     *
     * @param [in] want_to_read shard indexes to be decoded
     * @param [in] in map of available shard indexes to shard data
     * @param [out] out map of shard indexes that nede to be decoded to empty buffers
     * @return **0** on success or a negative errno on error.
     */
    virtual int decode_chunks(const shard_id_set &want_to_read,
                              shard_id_map<bufferptr> &in,
                              shard_id_map<bufferptr> &out) = 0;

    [[deprecated]]
    virtual int decode_chunks(const std::set<int> &want_to_read,
                              const std::map<int, bufferlist> &chunks,
                              std::map<int, bufferlist> *decoded) = 0;

    /**
     * Return the ordered list of chunks or an empty vector
     * if no remapping is necessary.
     *
     * By default encoding an object with K=2,M=1 will create three
     * chunks, the first two are data and the last one coding. For
     * a 10MB object, it would be:
     *
     *   chunk 0 for the first 5MB
     *   chunk 1 for the last 5MB
     *   chunk 2 for the 5MB coding chunk
     *
     * The plugin may, however, decide to remap them in a different
     * order, such as:
     *
     *   chunk 0 for the last 5MB
     *   chunk 1 for the 5MB coding chunk
     *   chunk 2 for the first 5MB
     *
     * The vector<int> remaps the chunks so that the first chunks are
     * data, in sequential order, and the last chunks contain parity
     * in the same order as they were output by the encoding function.
     *
     * In the example above the mapping would be:
     *
     *   [ 1, 2, 0 ]
     *
     * The returned vector<int> only contains information for chunks
     * that need remapping. If no remapping is necessary, the
     * vector<int> is empty.
     *
     * @return vector<int> list of indices of chunks to be remapped
     */
    virtual const std::vector<shard_id_t> &get_chunk_mapping() const = 0;

    /**
     * Decode the first **get_data_chunk_count()** **chunks** and
     * concatenate them into **decoded**.
     *
     * Returns 0 on success.
     *
     * @param [in] want_to_read mapped std::set of chunks caller wants
     *				concatenated to `decoded`. This works as
     *				selectors for `chunks`
     * @param [in] chunks set of chunks with data available for decoding
     * @param [out] decoded must be non-null, chunks specified in `want_to_read`
     * 			    will be concatenated into `decoded` in index order
     * @return **0** on success or a negative errno on error.
     */
    [[deprecated]]
    virtual int decode_concat(const std::set<int>& want_to_read,
			      const std::map<int, bufferlist> &chunks,
			      bufferlist *decoded) = 0;
    [[deprecated]]
    virtual int decode_concat(const std::map<int, bufferlist> &chunks,
			      bufferlist *decoded) = 0;

  	using plugin_flags = uint64_t;

    /**
     * Return a set of flags indicating which EC optimizations are supported
     * by the plugin.
     *
     * @return logical OR of the supported performance optimizations
     */
    virtual plugin_flags get_supported_optimizations() const = 0;
    enum {
      /* Partial read optimization assumes that the erasure code is systematic
       * and that concatenating the data chunks in the order returned by
       * get_chunk_mapping will create the data encoded for a stripe. The
       * optimization permits small reads to read data directly from the data
       * chunks without calling decode.
       */
      FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION = 1<<0,
      /* Partial write optimization assumes that a write to less than one
       * chunk only needs to read this fragment from each data chunk in the
       * stripe and can then use encode to create the corresponding coding
       * fragments.
       */
      FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION = 1<<1,
      /* Zero input zero output optimization means the erasure code has the
       * property that if all the data chunks are zero then the coding parity
       * chunks will also be zero.
       */
      FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION = 1<<2,
      /* Zero padding optimization permits the encode and decode methods to
       * be called with buffers that are zero length. The plugin treats
       * this as a chunk of all zeros.
       */
      FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION = 1<<3,
      /* Parity delta write optimization means the encode_delta and
       * apply_delta methods are supported which allows small updates
       * to a stripe to be applied using a read-modify-write of a
       * data chunk and the coding parity chunks.
       */
      FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION = 1<<4,
      /* This plugin requires sub-chunks (at the time of writing this was only
       * clay). Other plugins will not process the overhead of stub sub-chunks.
       */
      FLAG_EC_PLUGIN_REQUIRE_SUB_CHUNKS = 1<<5,
      /* Optimized EC is supported only if this flag is set. All other flags
       * are irrelevant if this flag is false.
       */
      FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED = 1<<6,
    };
    static const char *get_optimization_flag_name(const plugin_flags flag) {
      switch (flag) {
      case FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION: return "partialread";
      case FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION: return "partialwrite";
      case FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION: return "zeroinout";
      case FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION: return "zeropadding";
      case FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION: return "paritydelta";
      case FLAG_EC_PLUGIN_REQUIRE_SUB_CHUNKS: return "requiresubchunks";
      case FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED: return "optimizedsupport";
      default: return "???";
      }
    }
    static std::string get_optimization_flags_string(plugin_flags flags) {
      std::string s;
      for (unsigned n=0; flags && n<64; ++n) {
      	if (flags & (1ull << n)) {
			if (s.length())
				s += ",";
			s += get_optimization_flag_name(1ull << n);
			flags -= flags & (1ull << n);
		}
      }
      return s;
    }

    /**
     * Return a string describing which EC optimizations are supported
     * by the plugin.
     *
     * @return string of optimizations supported by the plugin
     */
    virtual std::string get_optimizations_flags_string() const {
      return get_optimization_flags_string(get_supported_optimizations());
    }
  };

  typedef std::shared_ptr<ErasureCodeInterface> ErasureCodeInterfaceRef;

}

#endif
