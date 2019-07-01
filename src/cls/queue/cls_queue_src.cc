// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue.h"

static int get_queue_head_and_size(cls_method_context_t hctx, cls_queue_head& head, uint64_t& head_size)
{
  //read head size
  bufferlist bl_head_size;
  int ret = cls_cxx_read(hctx, 0, sizeof(uint64_t), &bl_head_size);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: get_queue_head_and_size: failed to read head with error %d\n", ret);
    return ret;
  }
  //decode head size
  auto iter = bl_head_size.cbegin();
  try {
    decode(head_size, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: get_queue_head_and_size: failed to decode head size \n");
    return -EINVAL;
  }

  CLS_LOG(10, "INFO: get_queue_head_and_size: head size is %lu\n", head_size);

  //read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }

  //decode head
  iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: get_queue_head_and_size: failed to decode head\n");
    return -EINVAL;
  }

  return 0;
}

int cls_create_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_create_queue_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_create_queue(): failed to decode entry\n");
    return -EINVAL;
  }

  // create the object
  int ret = cls_cxx_create(hctx, true);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);
    return ret;
  }
  
  CLS_LOG(10, "INFO: cls_create_queue create queue of head size %lu", op.head_size);
  CLS_LOG(10, "INFO: cls_create_queue create queue of size %lu", op.head.queue_size);

  uint64_t head_size = QUEUE_HEAD_SIZE_1K;

  if (op.head.has_urgent_data) {
    if (op.head_size == 0) {
      head_size = QUEUE_HEAD_SIZE_4K;
      op.head.tail = op.head.front = QUEUE_START_OFFSET_4K;
      op.head.last_entry_offset = QUEUE_START_OFFSET_4K;
    } else {
      head_size = op.head_size;
      op.head.tail = op.head.front = head_size;
      op.head.last_entry_offset = head_size;
    }
  } else {
    head_size = QUEUE_HEAD_SIZE_1K;
    op.head.tail = op.head.front = QUEUE_START_OFFSET_1K;
    op.head.last_entry_offset = QUEUE_START_OFFSET_1K;
  }
  op.head.queue_size += head_size;

  CLS_LOG(10, "INFO: cls_create_queue queue actual size %lu", op.head.queue_size);
  CLS_LOG(10, "INFO: cls_create_queue head size %lu", head_size);
  CLS_LOG(10, "INFO: cls_create_queue queue front offset %lu", op.head.front);


  //encode head size
  bufferlist bl;
  encode(head_size, bl);
  CLS_LOG(0, "INFO: cls_create_queue head size %u", bl.length());

  //encode head
  bufferlist bl_head;
  encode(op.head, bl_head);

  bl.claim_append(bl_head);

  CLS_LOG(0, "INFO: cls_create_queue writing head of size %u", bl.length());
  ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_write returned %d", __func__, ret);
    return ret;
  }
  return 0;
}

int cls_init_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);

  //head is already initialized
  if (ret == 0) {
    CLS_LOG(1, "INFO: cls_init_queue_op(): head is already initialized\n");
    return 0;
  }

  if (ret < 0 && ret != -EINVAL) {
    return ret;
  }

  auto in_iter = in->cbegin();
  cls_init_queue_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_init_queue_op(): failed to decode entry\n");
    return -EINVAL;
  }

  CLS_LOG(10, "INFO: cls_init_queue_op init queue of head size %lu", op.head_size);
  CLS_LOG(10, "INFO: cls_init_queue_op init queue of size %lu", op.head.queue_size);

  head_size = QUEUE_HEAD_SIZE_1K;

  if (op.head.has_urgent_data) {
    if (op.head_size == 0) {
      head_size = QUEUE_HEAD_SIZE_4K;
      op.head.tail = op.head.front = QUEUE_START_OFFSET_4K;
      op.head.last_entry_offset = QUEUE_START_OFFSET_4K;
    } else {
      head_size = op.head_size;
      op.head.tail = op.head.front = head_size;
      op.head.last_entry_offset = head_size;
    }
  } else {
    head_size = QUEUE_HEAD_SIZE_1K;
    op.head.tail = op.head.front = QUEUE_START_OFFSET_1K;
    op.head.last_entry_offset = QUEUE_START_OFFSET_1K;
  }
  op.head.queue_size += head_size;

  CLS_LOG(10, "INFO: cls_init_queue_op queue actual size %lu", op.head.queue_size);
  CLS_LOG(10, "INFO: cls_init_queue_op head size %lu", head_size);
  CLS_LOG(10, "INFO: cls_init_queue_op queue front offset %lu", op.head.front);

  //encode head size
  bufferlist bl;
  encode(head_size, bl);
  CLS_LOG(0, "INFO: cls_init_queue_op head size %u", bl.length());

  //encode head
  bufferlist bl_head;
  encode(op.head, bl_head);

  bl.claim_append(bl_head);

  CLS_LOG(0, "INFO: cls_init_queue_op writing head of size %u", bl.length());
  ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_init_queue_op returned %d", __func__, ret);
    return ret;
  }
  return 0;
}

int cls_get_queue_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  head.queue_size -= head_size;

  CLS_LOG(10, "INFO: cls_get_queue_size: size of queue is %lu\n", head.queue_size);

  encode(head.queue_size, *out);

  return 0;
}

int cls_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  if ((head.front == head.tail) && (! head.is_empty)) {
    CLS_LOG(1, "ERROR: No space left in queue\n");
    return -ENOSPC;
  }

  auto iter = in->cbegin();
  cls_enqueue_op op;
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_enqueue: failed to decode input data \n");
    return -EINVAL;
  }

  for (auto bl_data : op.bl_data_vec) {
    bufferlist bl;
    uint64_t data_size = bl_data.length();
    encode(data_size, bl);
    bl.claim_append(bl_data);

    CLS_LOG(1, "INFO: cls_enqueue(): Total size to be written is %u and data size is %u\n", bl.length(), bl_data.length());

    if (head.tail >= head.front) {
      // check if data can fit in the remaining space in queue
      if ((head.tail + bl.length()) <= head.queue_size) {
        CLS_LOG(1, "INFO: cls_enqueue: Writing data size and data: offset: %lu, size: %u\n", head.tail, bl.length());
        head.last_entry_offset = head.tail;
        //write data size and data at tail offset
        ret = cls_cxx_write2(hctx, head.tail, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        head.tail += bl.length();
      } else {
        CLS_LOG(1, "INFO: Wrapping around and checking for free space\n");
        uint64_t free_space_available = (head.queue_size - head.tail) + (head.front - head_size);
        //Split data if there is free space available
        if (bl.length() <= free_space_available) {
          uint64_t size_before_wrap = head.queue_size - head.tail;
          bufferlist bl_data_before_wrap;
          bl.splice(0, size_before_wrap, &bl_data_before_wrap);
          head.last_entry_offset = head.tail;
          //write spliced (data size and data) at tail offset
          CLS_LOG(1, "INFO: cls_enqueue: Writing spliced data at offset: %lu and data size: %u\n", head.tail, bl_data_before_wrap.length());
          ret = cls_cxx_write2(hctx, head.tail, bl_data_before_wrap.length(), &bl_data_before_wrap, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
          if (ret < 0) {
            return ret;
          }
          head.tail = head_size;
          //write remaining data at tail offset after wrapping around
          CLS_LOG(1, "INFO: cls_enqueue: Writing remaining data at offset: %lu and data size: %u\n", head.tail, bl.length());
          ret = cls_cxx_write2(hctx, head.tail, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
          if (ret < 0) {
            return ret;
          }
          head.tail += bl.length();
        } else {
          CLS_LOG(1, "ERROR: No space left in queue\n");
          // return queue full error
          return -ENOSPC;
        }
      }
    } else if (head.front > head.tail) {
      if ((head.tail + bl.length()) <= head.front) {
        CLS_LOG(1, "INFO: cls_enqueue: Writing data size and data: offset: %lu, size: %u\n\n", head.tail, bl.length());
        head.last_entry_offset = head.tail;
        //write data size and data at tail offset
        ret = cls_cxx_write2(hctx, head.tail, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        head.tail += bl.length();
      } else {
        CLS_LOG(1, "ERROR: No space left in queue\n");
        // return queue full error
        return -ENOSPC;
      }
    }

    if (head.tail == head.queue_size) {
      head.tail = head_size;
    }
    CLS_LOG(1, "INFO: cls_enqueue: New tail offset: %lu \n", head.tail);
  } //end - for

  head.is_empty = false;

  //Update urgent data if set
  if (op.bl_urgent_data.length() > 0) {
    head.bl_urgent_data = op.bl_urgent_data;
  }

  bufferlist bl_head;
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_enqueue: Writing head of size: %u \n", bl_head.length());
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_enqueue: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

int cls_queue_list_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  // If queue is empty, return from here
  if (head.is_empty) {
    return -ENOENT;
  }

  cls_queue_list_ret op_ret;
  CLS_LOG(1, "INFO: cls_queue_list_entries: Is urgent data present: %d\n", head.has_urgent_data);
  //Info related to urgent data
  op_ret.bl_urgent_data = head.bl_urgent_data;

  auto in_iter = in->cbegin();

  cls_queue_list_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_list_entries(): failed to decode input data\n");
    return -EINVAL;
  }

  uint64_t start_offset = 0;
  if (op.start_offset == 0) {
    start_offset = head.front;
  } else {
    start_offset = op.start_offset;
  }

  op_ret.is_truncated = true;
  uint64_t chunk_size = 1024;
  uint64_t contiguous_data_size = 0, size_to_read = 0;
  bool wrap_around = false;

  //Calculate length of contiguous data to be read depending on front, tail and start offset
  if (head.tail > head.front) {
    contiguous_data_size = head.tail - start_offset;
  } else if (head.front >= head.tail) {
    if (start_offset >= head.front) {
      contiguous_data_size = head.queue_size - start_offset;
      wrap_around = true;
    } else if (start_offset <= head.tail) {
      contiguous_data_size = head.tail - start_offset;
    }
  }

  CLS_LOG(1, "INFO: cls_queue_list_entries(): front is: %lu, tail is %lu\n", head.front, head.tail);

  bool offset_populated = false;
  uint64_t num_ops = 0;
  bufferlist bl;
  do
  {
    CLS_LOG(1, "INFO: cls_queue_list_entries(): start_offset is %lu\n", start_offset);
  
    bufferlist bl_chunk;
    //Read chunk size at a time, if it is less than contiguous data size, else read contiguous data size
    if (contiguous_data_size > chunk_size) {
      size_to_read = chunk_size;
    } else {
      size_to_read = contiguous_data_size;
    }
    CLS_LOG(1, "INFO: cls_queue_list_entries(): size_to_read is %lu\n", size_to_read);
    if (size_to_read == 0) {
      op_ret.is_truncated = false;
      CLS_LOG(1, "INFO: cls_queue_list_entries(): size_to_read is 0, hence breaking out!\n");
      break;
    }

    ret = cls_cxx_read(hctx, start_offset, size_to_read, &bl_chunk);
    if (ret < 0) {
      return ret;
    }

    //If there is leftover data from previous iteration, append new data to leftover data
    bl.claim_append(bl_chunk);
    bl_chunk = bl;
    bl.clear();

    CLS_LOG(1, "INFO: cls_queue_list_entries(): size of chunk %u\n", bl_chunk.length());

    //Process the chunk of data read
    unsigned index = 0;
    auto it = bl_chunk.cbegin();
    uint64_t size_to_process = bl_chunk.length();
    do {
      CLS_LOG(1, "INFO: cls_queue_list_entries(): index: %u, size_to_process: %lu\n", index, size_to_process);
      it.seek(index);
      uint64_t data_size = 0;
      //Populate offset if not done, else don't if already done in previous iteration
      if (! offset_populated) {
        uint64_t data_offset = start_offset + index;
        op_ret.offsets.emplace_back(data_offset);
        CLS_LOG(1, "INFO: cls_queue_list_entries(): offset: %lu\n", data_offset);
      }
      if (size_to_process >= sizeof(uint64_t)) {
        try {
          decode(data_size, it);
        } catch (buffer::error& err) {
          CLS_LOG(1, "ERROR: cls_queue_list_entries: failed to decode data size \n");
          return -EINVAL;
        }
      } else {
        // Copy unprocessed data to bl
        bl_chunk.copy(index, size_to_process, bl);
        offset_populated = true;
        CLS_LOG(1, "INFO: cls_queue_list_entries: not enough data to read data size, breaking out!\n");
        break;
      }
      CLS_LOG(1, "INFO: cls_queue_list_entries(): data size: %lu\n", data_size);
      index += sizeof(uint64_t);
      size_to_process -= sizeof(uint64_t);
      bufferlist bl_data;
      if (data_size <= size_to_process) {
        bl_chunk.copy(index, data_size, bl_data);
        //Return data here
        op_ret.data.emplace_back(bl_data);
        index += bl_data.length();
        size_to_process -= bl_data.length();
      } else {
        index -= sizeof(uint64_t);
        size_to_process += sizeof(uint64_t);
        bl_chunk.copy(index, size_to_process, bl);
        offset_populated = true;
        CLS_LOG(1, "INFO: cls_queue_list_entries(): not enough data to read data, breaking out!\n");
        break;
      }
      offset_populated = false;
      num_ops++;
      if (num_ops == op.max) {
        CLS_LOG(1, "INFO: cls_queue_list_entries(): num_ops is same as op.max, hence breaking out from inner loop!\n");
        break;
      }
      if (index == bl_chunk.length()) {
        break;
      }
    } while(index < bl_chunk.length());

    CLS_LOG(1, "INFO: num_ops: %lu and op.max is %lu\n", num_ops, op.max);

    if (num_ops == op.max) {
      op_ret.next_offset = start_offset + index;
      CLS_LOG(1, "INFO: cls_queue_list_entries(): num_ops is same as op.max, hence breaking out from outer loop with next offset: %lu\n", op_ret.next_offset);
      break;
    }

    //Calculate new start_offset and contiguous data size
    start_offset += size_to_read;
    contiguous_data_size -= size_to_read;
    if (contiguous_data_size == 0) {
      if (wrap_around) {
        start_offset = head_size;
        contiguous_data_size = head.tail - head_size;
        wrap_around = false;
      } else {
        CLS_LOG(1, "INFO: cls_queue_list_entries(): end of queue data is reached, hence breaking out from outer loop!\n");
        op_ret.next_offset = head.front;
        op_ret.is_truncated = false;
        break;
      }
    }
    
  } while(num_ops < op.max);

  //Wrap around next offset if it has reached end of queue
  if (op_ret.next_offset == head.queue_size) {
    op_ret.next_offset = head_size;
  }
  if (op_ret.next_offset == head.tail) {
    op_ret.is_truncated = false;
  }

  encode(op_ret, *out);

  return 0;
}

int cls_queue_remove_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  if (head.is_empty) {
    return -ENOENT;
  }

  auto in_iter = in->cbegin();
  cls_queue_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode input data\n");
    return -EINVAL;
  }

  // If start offset is not set or set to zero, then we need to shift it to actual front of queue
  if (op.start_offset == 0) {
    op.start_offset = head.front;
  }

  if (op.start_offset != head.front) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: invalid start offset\n");
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: start offset = %lu\n", op.start_offset);
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: front = %lu\n", head.front);
    return -EINVAL;
  }

  uint64_t end_offset = 0;
  uint64_t data_size = 0;

  //Check for data size wrap around
  if ((head.queue_size - op.end_offset) >= sizeof(uint64_t)) {
    // Read the size from the end offset
    bufferlist bl_size;
    ret = cls_cxx_read(hctx, op.end_offset, sizeof(uint64_t), &bl_size);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to read data size \n");
      return ret;
    }
    auto iter = bl_size.cbegin();
    try {
      decode(data_size, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode data size \n");
      return -EINVAL;
    }
    //Check for data wrap around
    if ((head.queue_size - (op.end_offset + sizeof(uint64_t))) >= data_size) {
      end_offset = op.end_offset + sizeof(uint64_t) + data_size;
    } else {
      uint64_t rem_size = data_size - (head.queue_size - (op.end_offset + sizeof(uint64_t)));
      end_offset = head_size + rem_size;
    }
  } else {
    bufferlist bl_size;
    ret = cls_cxx_read(hctx, op.end_offset, (head.queue_size - op.end_offset), &bl_size);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to read first part of data size \n");
      return ret;
    }
    uint64_t rem_size = sizeof(uint64_t) - (head.queue_size - op.end_offset);
    bufferlist bl;
    ret = cls_cxx_read(hctx, head_size, rem_size, &bl);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to read second part of data size \n");
      return ret;
    }
    bl_size.claim_append(bl);
    auto iter = bl_size.cbegin();
    try {
      decode(data_size, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode data size \n");
      return -EINVAL;
    }
    end_offset = head_size + rem_size + data_size;
  }

  CLS_LOG(1, "INFO: cls_queue_remove_entries: op.end_offset = %lu\n", op.end_offset);
  CLS_LOG(1, "INFO: cls_queue_remove_entries: data_size = %lu\n", data_size);
  CLS_LOG(1, "INFO: cls_queue_remove_entries: end_offset = %lu\n", end_offset);

  //Zero out the entries that have been removed, to reclaim storage space
  if (end_offset > op.start_offset) {
    ret = cls_cxx_write_zero(hctx, op.start_offset, (end_offset - op.start_offset));
    if (ret < 0) {
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Failed to zero out entries\n");
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Start offset = %lu\n", op.start_offset);
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Length = %lu\n", (end_offset - op.start_offset));
      return ret;
    }
  } else { //start offset > end offset
    ret = cls_cxx_write_zero(hctx, op.start_offset, (head.queue_size - op.start_offset));
    if (ret < 0) {
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Failed to zero out entries\n");
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Start offset = %lu\n", op.start_offset);
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Length = %lu\n", (head.queue_size - op.start_offset));
      return ret;
    }
    ret = cls_cxx_write_zero(hctx, head_size, (end_offset - head_size));
    if (ret < 0) {
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Failed to zero out entries\n");
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Start offset = %lu\n", head_size);
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Length = %lu\n", (end_offset - head_size));
      return ret;
    }
  }

  head.front = end_offset;

  // Check if it is the end, then wrap around
  if (head.front == head.queue_size) {
    head.front = head_size;
  }

  CLS_LOG(1, "INFO: cls_queue_remove_entries: front offset is: %lu and tail offset is %lu\n", head.front, head.tail);

  // We've reached the last element
  if (head.front == head.tail) {
    CLS_LOG(1, "INFO: cls_queue_remove_entries: Queue is empty now!\n");
    head.is_empty = true;
  }

  //Update urgent data map
  if (op.bl_urgent_data.length() > 0) {
    head.bl_urgent_data = op.bl_urgent_data;
  }

  //Write head back
  bufferlist bl_head;
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_remove_entries: Writing head of size: %u and front offset is: %lu\n", bl_head.length(), head.front);
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_remove_entries: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

int cls_queue_get_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  uint64_t data_size = 0, last_entry_offset = head.last_entry_offset;

  //Check for data size wrap around
  if ((head.queue_size - last_entry_offset) >= sizeof(uint64_t)) {
    // Read the size from the end offset
    bufferlist bl_size;
    ret = cls_cxx_read(hctx, last_entry_offset, sizeof(uint64_t), &bl_size);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_get_last_entry: failed to read data size \n");
      return ret;
    }
    auto iter = bl_size.cbegin();
    try {
      decode(data_size, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_queue_get_last_entry: failed to decode data size \n");
      return -EINVAL;
    }
    last_entry_offset += sizeof(uint64_t);
    //Check for data wrap around
    if ((head.queue_size - (last_entry_offset + sizeof(uint64_t))) >= data_size) {
      CLS_LOG(1, "INFO: cls_queue_get_last_entry: Data is read from from last entry offset %lu\n", last_entry_offset);
      ret = cls_cxx_read2(hctx, last_entry_offset, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0) {
        return ret;
      }
    } else {
      CLS_LOG(1, "INFO: cls_queue_get_last_entry: Data is read from from last entry offset %lu\n", last_entry_offset);
      ret = cls_cxx_read2(hctx, last_entry_offset, (head.queue_size - (last_entry_offset + sizeof(uint64_t))), out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0) {
        return ret;
      }
      uint64_t rem_size = data_size - (head.queue_size - (last_entry_offset + sizeof(uint64_t)));
      bufferlist bl;
      ret = cls_cxx_read(hctx, head_size, rem_size, &bl);
      if (ret < 0) {
        return ret;
      }
      out->claim_append(bl);
    }
  } else {
    bufferlist bl_size;
    ret = cls_cxx_read(hctx, last_entry_offset, (head.queue_size - last_entry_offset), &bl_size);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to read first part of data size \n");
      return ret;
    }
    uint64_t rem_size = sizeof(uint64_t) - (head.queue_size - last_entry_offset);
    bufferlist bl;
    ret = cls_cxx_read(hctx, head_size, rem_size, &bl);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to read second part of data size \n");
      return ret;
    }
    bl_size.claim_append(bl);
    auto iter = bl_size.cbegin();
    try {
      decode(data_size, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode data size \n");
      return -EINVAL;
    }
    last_entry_offset = head_size + rem_size;
    CLS_LOG(1, "INFO: cls_queue_get_last_entry: Data is read from from last entry offset %lu\n", last_entry_offset);
    ret = cls_cxx_read2(hctx, last_entry_offset, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

int cls_queue_update_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  auto in_iter = in->cbegin();

  cls_queue_update_last_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_update_last_entry: failed to decode input data\n");
    return -EINVAL;
  }

  bufferlist bl;
  uint64_t data_size = op.bl_data.length();
  encode(data_size, bl);
  bl.claim_append(op.bl_data);

  CLS_LOG(1, "INFO: cls_queue_update_last_entry_op: Updating data at last offset: %lu and total data size is %u\n", head.last_entry_offset, bl.length());

  // check if data can fit in the remaining space in queue
  if ((head.last_entry_offset + bl.length()) <= head.queue_size) {
    //write data size and data at tail offset
    ret = cls_cxx_write2(hctx, head.last_entry_offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      return ret;
    }
  } else {
    CLS_LOG(1, "INFO: Wrapping around and checking for free space\n");
    uint64_t free_space_available = (head.queue_size - head.last_entry_offset) + (head.front - head_size);
    //Split data if there is free space available
    if (bl.length() <= free_space_available) {
      uint64_t size_before_wrap = head.queue_size - head.last_entry_offset;
      bufferlist bl_data_before_wrap;
      bl.splice(0, size_before_wrap, &bl_data_before_wrap);
      //write spliced (data size and data) at last entry offset
      CLS_LOG(1, "INFO: cls_enqueue: Writing spliced data at offset: %lu and data size: %u\n", head.last_entry_offset, bl_data_before_wrap.length());
      ret = cls_cxx_write2(hctx, head.last_entry_offset, bl_data_before_wrap.length(), &bl_data_before_wrap, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0) {
        return ret;
      }
      //write remaining data after wrapping around
      CLS_LOG(1, "INFO: cls_enqueue: Writing remaining data at offset: %lu and data size: %u\n", head_size, bl.length());
      ret = cls_cxx_write2(hctx, head_size, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0) {
        return ret;
      }
    }
    else {
      CLS_LOG(1, "ERROR: No space left in queue\n");
      // return queue full error
      return -ENOSPC;
    }
  }

  if (op.bl_urgent_data.length() > 0) {
    head.bl_urgent_data = op.bl_urgent_data;
  }

  bufferlist bl_head;
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_update_last_entry: Writing head of size: %u \n", bl_head.length());
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_update_last_entry: Writing head returned error: %d \n", ret);
    return ret;
  }
  return 0;
}

int cls_queue_read_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  CLS_LOG(1, "INFO: cls_queue_read_urgent_data: tail offset %lu\n", head.tail);

  cls_queue_urgent_data_ret op_ret;
  
  op_ret.bl_urgent_data = head.bl_urgent_data;
  
  encode(op_ret, *out);

  return 0;
}

int cls_queue_write_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  CLS_LOG(1, "INFO: cls_queue_write_urgent_data: tail offset %lu\n", head.tail);

  auto in_iter = in->cbegin();

  cls_queue_write_urgent_data_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_write_urgent_data: failed to decode input data\n");
    return -EINVAL;
  }
  //Write urgent data
  head.bl_urgent_data = op.bl_urgent_data;

  //Write head back
  bufferlist bl_head;
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_write_urgent_data: Writing head of size: %u\n", bl_head.length());
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_write_urgent_data: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

int cls_queue_can_urgent_data_fit(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bool can_fit = true;

  //get head and its size
  uint64_t head_size = 0;
  cls_queue_head head;
  int ret = get_queue_head_and_size(hctx, head, head_size);
  if (ret < 0) {
    return ret;
  }

  head.bl_urgent_data = *in;

  bufferlist bl_head;
  encode(head, bl_head);

  if(bl_head.length() > head_size) {
    can_fit = false;
  }

  encode(can_fit, *out);

  return 0;
}

