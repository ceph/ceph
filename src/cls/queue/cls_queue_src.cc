// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "objclass/objclass.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue_src.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

int queue_write_head(cls_method_context_t hctx, cls_queue_head& head)
{
  bufferlist bl;
  uint16_t entry_start = QUEUE_HEAD_START;
  encode(entry_start, bl);

  bufferlist bl_head;
  encode(head, bl_head);

  uint64_t encoded_len = bl_head.length();
  encode(encoded_len, bl);

  bl.claim_append(bl_head);

  int ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(5, "ERROR: queue_write_head: failed to write head");
    return ret;
  }
  return 0;
}

int queue_read_head(cls_method_context_t hctx, cls_queue_head& head)
{
  uint64_t chunk_size = 1024, start_offset = 0;

  bufferlist bl_head;
  const auto  ret = cls_cxx_read(hctx, start_offset, chunk_size, &bl_head);
  if (ret < 0) {
    CLS_LOG(5, "ERROR: queue_read_head: failed to read head");
    return ret;
  }
  if (ret == 0) {
    CLS_LOG(20, "INFO: queue_read_head: empty head, not initialized yet");
    return -EINVAL;
  }

  //Process the chunk of data read
  auto it = bl_head.cbegin();
  // Queue head start
  uint16_t queue_head_start;
  try {
    decode(queue_head_start, it);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: queue_read_head: failed to decode queue start: %s", err.what());
    return -EINVAL;
  }
  if (queue_head_start != QUEUE_HEAD_START) {
    CLS_LOG(0, "ERROR: queue_read_head: invalid queue start");
    return -EINVAL;
  }

  uint64_t encoded_len;
  try {
    decode(encoded_len, it);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: queue_read_head: failed to decode encoded head size: %s", err.what());
    return -EINVAL;
  }

  if (encoded_len > (chunk_size - QUEUE_ENTRY_OVERHEAD)) {
    start_offset = chunk_size;
    chunk_size = (encoded_len - (chunk_size - QUEUE_ENTRY_OVERHEAD));
    bufferlist bl_remaining_head;
    const auto ret = cls_cxx_read2(hctx, start_offset, chunk_size, &bl_remaining_head, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      CLS_LOG(5, "ERROR: queue_read_head: failed to read remaining part of head");
      return ret;
    }
    bl_head.claim_append(bl_remaining_head);
  }

  try {
    decode(head, it);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: queue_read_head: failed to decode head: %s", err.what());
    return -EINVAL;
  }

  return 0;
}

int queue_init(cls_method_context_t hctx, const cls_queue_init_op& op)
{
  //get head and its size
  cls_queue_head head;
  int ret = queue_read_head(hctx, head);

  //head is already initialized
  if (ret == 0) {
    return -EEXIST;
  }

  if (ret < 0 && ret != -EINVAL) {
    return ret;
  }

  if (op.bl_urgent_data.length() > 0) {
    head.bl_urgent_data = op.bl_urgent_data;
  }

  head.max_head_size = QUEUE_HEAD_SIZE_1K + op.max_urgent_data_size;
  head.queue_size = op.queue_size + head.max_head_size;
  head.max_urgent_data_size = op.max_urgent_data_size;
  head.tail.gen = head.front.gen = 0;
  head.tail.offset = head.front.offset = head.max_head_size;
  
  CLS_LOG(20, "INFO: init_queue_op queue actual size %lu", head.queue_size);
  CLS_LOG(20, "INFO: init_queue_op head size %lu", head.max_head_size);
  CLS_LOG(20, "INFO: init_queue_op queue front offset %s", head.front.to_str().c_str());
  CLS_LOG(20, "INFO: init_queue_op queue max urgent data size %lu", head.max_urgent_data_size);

  return queue_write_head(hctx, head);
}

int queue_get_capacity(cls_method_context_t hctx, cls_queue_get_capacity_ret& op_ret)
{
  //get head
  cls_queue_head head;
  int ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  op_ret.queue_capacity = head.queue_size - head.max_head_size;

  CLS_LOG(20, "INFO: queue_get_capacity: size of queue is %lu", op_ret.queue_capacity);

  return 0;
}


/*
enqueue of new bufferlist happens in the free spaces of the queue, the queue can be in
one of two states:

(1) split free space
+-------------+--------------------------------------------------------------------+
| object head |                XXXXXXXXXXXXXXXXXXXXXXXXXXX                         |
|             |                ^                         ^                         |
| front  tail |                |                         |                         |
+---+------+--+----------------|-------------------------|-------------------------+
    |      |                   |                         |
    |      +-------------------|-------------------------+
    +--------------------------+

(2) continuous free space
+-------------+--------------------------------------------------------------------+
| object head |XXXXXXXXXXXXXXXXX                         XXXXXXXXXXXXXXXXXXXXXXXXXX|
|             |                ^                         ^                         |
| front  tail |                |                         |                         |
+---+------+--+----------------|-------------------------|-------------------------+
    |      |                   |                         |
    |      +-------------------+                         |
    +----------------------------------------------------+
*/

int queue_enqueue(cls_method_context_t hctx, cls_queue_enqueue_op& op, cls_queue_head& head)
{
  if ((head.front.offset == head.tail.offset) && (head.tail.gen == head.front.gen + 1)) {
    CLS_LOG(0, "ERROR: No space left in queue");
    return -ENOSPC;
  }

  for (auto& bl_data : op.bl_data_vec) {
    bufferlist bl;
    uint16_t entry_start = QUEUE_ENTRY_START;
    encode(entry_start, bl);
    uint64_t data_size = bl_data.length();
    encode(data_size, bl);
    bl.claim_append(bl_data);
  
    CLS_LOG(10, "INFO: queue_enqueue(): Total size to be written is %u and data size is %lu", bl.length(), data_size);

    if (head.tail.offset >= head.front.offset) {
      // check if data can fit in the remaining space in queue
      if ((head.tail.offset + bl.length()) <= head.queue_size) {
        CLS_LOG(5, "INFO: queue_enqueue: Writing data size and data: offset: %s, size: %u", head.tail.to_str().c_str(), bl.length());
        //write data size and data at tail offset
        auto ret = cls_cxx_write2(hctx, head.tail.offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        head.tail.offset += bl.length();
      } else {
        uint64_t free_space_available = (head.queue_size - head.tail.offset) + (head.front.offset - head.max_head_size);
        //Split data if there is free space available
        if (bl.length() <= free_space_available) {
          uint64_t size_before_wrap = head.queue_size - head.tail.offset;
          bufferlist bl_data_before_wrap;
          bl.splice(0, size_before_wrap, &bl_data_before_wrap);
          //write spliced (data size and data) at tail offset
          CLS_LOG(5, "INFO: queue_enqueue: Writing spliced data at offset: %s and data size: %u", head.tail.to_str().c_str(), bl_data_before_wrap.length());
          auto ret = cls_cxx_write2(hctx, head.tail.offset, bl_data_before_wrap.length(), &bl_data_before_wrap, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
          if (ret < 0) {
            return ret;
          }
          head.tail.offset = head.max_head_size;
          head.tail.gen += 1;
          //write remaining data at tail offset after wrapping around
          CLS_LOG(5, "INFO: queue_enqueue: Writing remaining data at offset: %s and data size: %u", head.tail.to_str().c_str(), bl.length());
          ret = cls_cxx_write2(hctx, head.tail.offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
          if (ret < 0) {
            return ret;
          }
          head.tail.offset += bl.length();
        } else {
          CLS_LOG(0, "ERROR: No space left in queue\n");
          // return queue full error
          return -ENOSPC;
        }
      }
    } else if (head.front.offset > head.tail.offset) {
      if ((head.tail.offset + bl.length()) <= head.front.offset) {
        CLS_LOG(5, "INFO: queue_enqueue: Writing data size and data: offset: %s, size: %u", head.tail.to_str().c_str(), bl.length());
        //write data size and data at tail offset
        auto ret = cls_cxx_write2(hctx, head.tail.offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        head.tail.offset += bl.length();
      } else {
        CLS_LOG(0, "ERROR: No space left in queue");
        // return queue full error
        return -ENOSPC;
      }
    }

    if (head.tail.offset == head.queue_size) {
      head.tail.offset = head.max_head_size;
      head.tail.gen += 1;
    }
    CLS_LOG(20, "INFO: queue_enqueue: New tail offset: %s", head.tail.to_str().c_str());
  } //end - for

  return 0;
}

int queue_list_entries(cls_method_context_t hctx, const cls_queue_list_op& op, cls_queue_list_ret& op_ret, cls_queue_head& head)
{
  // If queue is empty, return from here
  if ((head.front.offset == head.tail.offset) && (head.front.gen == head.tail.gen)) {
    CLS_LOG(20, "INFO: queue_list_entries(): Next offset is %s", head.front.to_str().c_str());
    op_ret.next_marker = head.front.to_str();
    op_ret.is_truncated = false;
    return 0;
  }

  cls_queue_marker start_marker;
  start_marker.from_str(op.start_marker.c_str());
  cls_queue_marker next_marker = {0, 0};

  uint64_t start_offset = 0, gen = 0;
  if (start_marker.offset == 0) {
    start_offset = head.front.offset;
    gen = head.front.gen;
  } else {
    start_offset = start_marker.offset;
    gen = start_marker.gen;
  }

  op_ret.is_truncated = true;
  uint64_t chunk_size = 1024;
  uint64_t contiguous_data_size = 0, size_to_read = 0;
  bool wrap_around = false;

  //Calculate length of contiguous data to be read depending on front, tail and start offset
  if (head.tail.offset > head.front.offset) {
    contiguous_data_size = head.tail.offset - start_offset;
  } else if (head.front.offset >= head.tail.offset) {
    if (start_offset >= head.front.offset) {
      contiguous_data_size = head.queue_size - start_offset;
      wrap_around = true;
    } else if (start_offset <= head.tail.offset) {
      contiguous_data_size = head.tail.offset - start_offset;
    }
  }

  CLS_LOG(10, "INFO: queue_list_entries(): front is: %s, tail is %s", head.front.to_str().c_str(), head.tail.to_str().c_str());

  bool offset_populated = false, entry_start_processed = false;
  uint64_t data_size = 0, num_ops = 0;
  uint16_t entry_start = 0;
  bufferlist bl;
  string last_marker;
  do
  {
    CLS_LOG(10, "INFO: queue_list_entries(): start_offset is %lu", start_offset);
  
    bufferlist bl_chunk;
    //Read chunk size at a time, if it is less than contiguous data size, else read contiguous data size
    if (contiguous_data_size > chunk_size) {
      size_to_read = chunk_size;
    } else {
      size_to_read = contiguous_data_size;
    }
    CLS_LOG(10, "INFO: queue_list_entries(): size_to_read is %lu", size_to_read);
    if (size_to_read == 0) {
      next_marker = head.tail;
      op_ret.is_truncated = false;
      CLS_LOG(20, "INFO: queue_list_entries(): size_to_read is 0, hence breaking out!\n");
      break;
    }

    auto ret = cls_cxx_read(hctx, start_offset, size_to_read, &bl_chunk);
    if (ret < 0) {
      return ret;
    }

    //If there is leftover data from previous iteration, append new data to leftover data
    uint64_t entry_start_offset = start_offset - bl.length();
    CLS_LOG(20, "INFO: queue_list_entries(): Entry start offset accounting for leftover data is %lu", entry_start_offset);
    bl.claim_append(bl_chunk);
    bl_chunk = std::move(bl);

    CLS_LOG(20, "INFO: queue_list_entries(): size of chunk %u", bl_chunk.length());

    //Process the chunk of data read
    unsigned index = 0;
    auto it = bl_chunk.cbegin();
    uint64_t size_to_process = bl_chunk.length();
    do {
      CLS_LOG(10, "INFO: queue_list_entries(): index: %u, size_to_process: %lu", index, size_to_process);
      cls_queue_entry entry;
      ceph_assert(it.get_off() == index);
      //Use the last marker saved in previous iteration as the marker for this entry
      if (offset_populated) {
        entry.marker = last_marker;
      }
      //Populate offset if not done in previous iteration
      if (! offset_populated) {
        cls_queue_marker marker = {entry_start_offset + index, gen};
        CLS_LOG(5, "INFO: queue_list_entries(): offset: %s\n", marker.to_str().c_str());
        entry.marker = marker.to_str();
      }
      // Magic number + Data size - process if not done in previous iteration
      if (! entry_start_processed ) {
        if (size_to_process >= QUEUE_ENTRY_OVERHEAD) {
          // Decode magic number at start
          try {
            decode(entry_start, it);
          } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: queue_list_entries: failed to decode entry start: %s", err.what());
            return -EINVAL;
          }
          if (entry_start != QUEUE_ENTRY_START) {
            CLS_LOG(5, "ERROR: queue_list_entries: invalid entry start %u", entry_start);
            return -EINVAL;
          }
          index += sizeof(uint16_t);
          size_to_process -= sizeof(uint16_t);
          // Decode data size
          try {
            decode(data_size, it);
          } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: queue_list_entries: failed to decode data size: %s", err.what());
            return -EINVAL;
          }
        } else {
          // Copy unprocessed data to bl
          bl_chunk.splice(index, size_to_process, &bl);
          offset_populated = true;
          last_marker = entry.marker;
          CLS_LOG(10, "INFO: queue_list_entries: not enough data to read entry start and data size, breaking out!");
          break;
        }
        CLS_LOG(20, "INFO: queue_list_entries(): data size: %lu", data_size);
        index += sizeof(uint64_t);
        size_to_process -= sizeof(uint64_t);
      }
      // Data
      if (data_size <= size_to_process) {
        it.copy(data_size, entry.data);
        index += entry.data.length();
        size_to_process -= entry.data.length();
      } else {
        it.copy(size_to_process, bl);
        offset_populated = true;
        entry_start_processed = true;
        last_marker = entry.marker;
        CLS_LOG(10, "INFO: queue_list_entries(): not enough data to read data, breaking out!");
        break;
      }
      op_ret.entries.emplace_back(entry);
      // Resetting some values
      offset_populated = false;
      entry_start_processed = false;
      data_size = 0;
      entry_start = 0;
      num_ops++;
      last_marker.clear();
      if (num_ops == op.max) {
        CLS_LOG(10, "INFO: queue_list_entries(): num_ops is same as op.max, hence breaking out from inner loop!");
        break;
      }
    } while(index < bl_chunk.length());

    CLS_LOG(10, "INFO: num_ops: %lu and op.max is %lu\n", num_ops, op.max);

    if (num_ops == op.max) {
      next_marker = cls_queue_marker{(entry_start_offset + index), gen};
      CLS_LOG(10, "INFO: queue_list_entries(): num_ops is same as op.max, hence breaking out from outer loop with next offset: %lu", next_marker.offset);
      break;
    }

    //Calculate new start_offset and contiguous data size
    start_offset += size_to_read;
    contiguous_data_size -= size_to_read;
    if (contiguous_data_size == 0) {
      if (wrap_around) {
        start_offset = head.max_head_size;
        contiguous_data_size = head.tail.offset - head.max_head_size;
        gen += 1;
        wrap_around = false;
      } else {
        CLS_LOG(10, "INFO: queue_list_entries(): end of queue data is reached, hence breaking out from outer loop!");
        next_marker = head.tail;
        op_ret.is_truncated = false;
        break;
      }
    }
    
  } while(num_ops < op.max);

  //Wrap around next offset if it has reached end of queue
  if (next_marker.offset == head.queue_size) {
    next_marker.offset = head.max_head_size;
    next_marker.gen += 1;
  }
  if ((next_marker.offset == head.tail.offset) && (next_marker.gen == head.tail.gen)) {
    op_ret.is_truncated = false;
  }

  CLS_LOG(5, "INFO: queue_list_entries(): next offset: %s", next_marker.to_str().c_str());
  op_ret.next_marker = next_marker.to_str();

  return 0;
}

int queue_remove_entries(cls_method_context_t hctx, const cls_queue_remove_op& op, cls_queue_head& head)
{
  //Queue is empty
  if ((head.front.offset == head.tail.offset) && (head.front.gen == head.tail.gen)) {
    return 0;
  }

  cls_queue_marker end_marker;
  end_marker.from_str(op.end_marker.c_str());

  CLS_LOG(5, "INFO: queue_remove_entries: op.end_marker = %s", end_marker.to_str().c_str());

  //Zero out the entries that have been removed, to reclaim storage space
  if (end_marker.offset > head.front.offset && end_marker.gen == head.front.gen) {
    uint64_t len = end_marker.offset - head.front.offset;
    if (len > 0) {
      auto ret = cls_cxx_write_zero(hctx, head.front.offset, len);
      if (ret < 0) {
        CLS_LOG(5, "INFO: queue_remove_entries: Failed to zero out entries");
        CLS_LOG(10, "INFO: queue_remove_entries: Start offset = %s", head.front.to_str().c_str());
        return ret;
      }
    }
  } else if ((head.front.offset >= end_marker.offset) && (end_marker.gen == head.front.gen + 1)) { //start offset > end offset
    uint64_t len = head.queue_size - head.front.offset;
    if (len > 0) {
      auto ret = cls_cxx_write_zero(hctx, head.front.offset, len);
      if (ret < 0) {
        CLS_LOG(5, "INFO: queue_remove_entries: Failed to zero out entries");
        CLS_LOG(10, "INFO: queue_remove_entries: Start offset = %s", head.front.to_str().c_str());
        return ret;
      }
    }
    len = end_marker.offset - head.max_head_size;
    if (len > 0) {
      auto ret = cls_cxx_write_zero(hctx, head.max_head_size, len);
      if (ret < 0) {
        CLS_LOG(5, "INFO: queue_remove_entries: Failed to zero out entries");
        CLS_LOG(10, "INFO: queue_remove_entries: Start offset = %lu", head.max_head_size);
        return ret;
      }
    }
  } else if ((head.front.offset == end_marker.offset) && (head.front.gen == end_marker.gen)) {
    //no-op
  } else {
    CLS_LOG(0, "INFO: queue_remove_entries: Invalid end marker: offset = %s, gen = %lu", end_marker.to_str().c_str(), end_marker.gen);
    return -EINVAL;
  }

  head.front = end_marker;

  // Check if it is the end, then wrap around
  if (head.front.offset == head.queue_size) {
    head.front.offset = head.max_head_size;
    head.front.gen += 1;
  }

  CLS_LOG(20, "INFO: queue_remove_entries: front offset is: %s and tail offset is %s", head.front.to_str().c_str(), head.tail.to_str().c_str());

  return 0;
}
