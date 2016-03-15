// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/errno.h"
#include "objclass/objclass.h"
#include "cls/journal/cls_journal_types.h"
#include <errno.h>
#include <map>
#include <string>
#include <sstream>

CLS_VER(1, 0)
CLS_NAME(journal)

cls_handle_t h_class;
cls_method_handle_t h_journal_create;
cls_method_handle_t h_journal_get_order;
cls_method_handle_t h_journal_get_splay_width;
cls_method_handle_t h_journal_get_pool_id;
cls_method_handle_t h_journal_get_minimum_set;
cls_method_handle_t h_journal_set_minimum_set;
cls_method_handle_t h_journal_get_active_set;
cls_method_handle_t h_journal_set_active_set;
cls_method_handle_t h_journal_get_client;
cls_method_handle_t h_journal_client_register;
cls_method_handle_t h_journal_client_update_data;
cls_method_handle_t h_journal_client_update_state;
cls_method_handle_t h_journal_client_unregister;
cls_method_handle_t h_journal_client_commit;
cls_method_handle_t h_journal_client_list;
cls_method_handle_t h_journal_get_next_tag_tid;
cls_method_handle_t h_journal_get_tag;
cls_method_handle_t h_journal_tag_create;
cls_method_handle_t h_journal_tag_list;
cls_method_handle_t h_journal_object_guard_append;

namespace {

static const uint64_t MAX_KEYS_READ = 64;

static const std::string HEADER_KEY_ORDER          = "order";
static const std::string HEADER_KEY_SPLAY_WIDTH    = "splay_width";
static const std::string HEADER_KEY_POOL_ID        = "pool_id";
static const std::string HEADER_KEY_MINIMUM_SET    = "minimum_set";
static const std::string HEADER_KEY_ACTIVE_SET     = "active_set";
static const std::string HEADER_KEY_NEXT_TAG_TID   = "next_tag_tid";
static const std::string HEADER_KEY_NEXT_TAG_CLASS = "next_tag_class";
static const std::string HEADER_KEY_CLIENT_PREFIX  = "client_";
static const std::string HEADER_KEY_TAG_PREFIX     = "tag_";

std::string to_hex(uint64_t value) {
  std::ostringstream oss;
  oss << std::setw(16) << std::setfill('0') << std::hex << value;
  return oss.str();
}

std::string key_from_client_id(const std::string &client_id) {
  return HEADER_KEY_CLIENT_PREFIX + client_id;
}

std::string key_from_tag_tid(uint64_t tag_tid) {
  return HEADER_KEY_TAG_PREFIX + to_hex(tag_tid);
}

uint64_t tag_tid_from_key(const std::string &key) {
  std::istringstream iss(key);
  uint64_t id;
  iss.ignore(HEADER_KEY_TAG_PREFIX.size()) >> std::hex >> id;
  return id;
}

template <typename T>
int read_key(cls_method_context_t hctx, const string &key, T *t,
             bool ignore_enoent = false) {
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r == -ENOENT && ignore_enoent) {
    return 0;
  } else if (r < 0) {
    CLS_ERR("failed to get omap key: %s", key.c_str());
    return r;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*t, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }
  return 0;
}

template <typename T>
int write_key(cls_method_context_t hctx, const string &key, const T &t) {
  bufferlist bl;
  ::encode(t, bl);

  int r = cls_cxx_map_set_val(hctx, key, &bl);
  if (r < 0) {
    CLS_ERR("failed to set omap key: %s", key.c_str());
    return r;
  }
  return 0;
}

int remove_key(cls_method_context_t hctx, const string &key) {
  int r = cls_cxx_map_remove_key(hctx, key);
  if (r < 0 && r != -ENOENT) {
      CLS_ERR("failed to remove key: %s", key.c_str());
      return r;
  }
  return 0;
}

int expire_tags(cls_method_context_t hctx, const std::string *skip_client_id) {

  std::string skip_client_key;
  if (skip_client_id != nullptr) {
    skip_client_key = key_from_client_id(*skip_client_id);
  }

  int r;
  uint64_t minimum_tag_tid = std::numeric_limits<uint64_t>::max();
  std::string last_read = HEADER_KEY_CLIENT_PREFIX;
  do {
    std::map<std::string, bufferlist> vals;
    r = cls_cxx_map_get_vals(hctx, last_read, HEADER_KEY_CLIENT_PREFIX,
                             MAX_KEYS_READ, &vals);
    if (r < 0 && r != -ENOENT) {
      CLS_ERR("failed to retrieve registered clients: %s",
              cpp_strerror(r).c_str());
      return r;
    }

    for (auto &val : vals) {
      // if we are removing a client, skip its commit positions
      if (val.first == skip_client_key) {
        continue;
      }

      cls::journal::Client client;
      bufferlist::iterator iter = val.second.begin();
      try {
        ::decode(client, iter);
      } catch (const buffer::error &err) {
        CLS_ERR("error decoding registered client: %s",
                val.first.c_str());
        return -EIO;
      }

      for (auto object_position : client.commit_position.object_positions) {
        minimum_tag_tid = MIN(minimum_tag_tid, object_position.tag_tid);
      }
    }
    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  } while (r == MAX_KEYS_READ);

  // cannot expire tags if a client hasn't committed yet
  if (minimum_tag_tid == std::numeric_limits<uint64_t>::max()) {
    return 0;
  }

  // compute the minimum in-use tag for each class
  std::map<uint64_t, uint64_t> minimum_tag_class_to_tids;
  typedef enum { TAG_PASS_CALCULATE_MINIMUMS,
                 TAG_PASS_SCRUB,
                 TAG_PASS_DONE } TagPass;
  int tag_pass = TAG_PASS_CALCULATE_MINIMUMS;
  last_read = HEADER_KEY_TAG_PREFIX;
  do {
    std::map<std::string, bufferlist> vals;
    r = cls_cxx_map_get_vals(hctx, last_read, HEADER_KEY_TAG_PREFIX,
                             MAX_KEYS_READ, &vals);
    if (r < 0 && r != -ENOENT) {
      CLS_ERR("failed to retrieve tags: %s", cpp_strerror(r).c_str());
      return r;
    }

    for (auto &val : vals) {
      cls::journal::Tag tag;
      bufferlist::iterator iter = val.second.begin();
      try {
        ::decode(tag, iter);
      } catch (const buffer::error &err) {
        CLS_ERR("error decoding tag: %s", val.first.c_str());
        return -EIO;
      }

      if (tag.tid != tag_tid_from_key(val.first)) {
        CLS_ERR("tag tid mismatched: %s", val.first.c_str());
        return -EINVAL;
      }

      if (tag_pass == TAG_PASS_CALCULATE_MINIMUMS) {
        minimum_tag_class_to_tids[tag.tag_class] = tag.tid;
      } else if (tag_pass == TAG_PASS_SCRUB &&
                 tag.tid < minimum_tag_class_to_tids[tag.tag_class]) {
        r = remove_key(hctx, val.first);
        if (r < 0) {
          return r;
        }
      }

      if (tag.tid >= minimum_tag_tid) {
        // no need to check for tag classes beyond this point
        vals.clear();
        break;
      }
    }

    if (tag_pass != TAG_PASS_DONE && vals.size() < MAX_KEYS_READ) {
      last_read = HEADER_KEY_TAG_PREFIX;
      ++tag_pass;
    } else if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  } while (tag_pass != TAG_PASS_DONE);
  return 0;
}

} // anonymous namespace

/**
 * Input:
 * @param order (uint8_t) - bits to shift to compute the object max size
 * @param splay width (uint8_t) - number of active journal objects
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_create(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  uint8_t order;
  uint8_t splay_width;
  int64_t pool_id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(order, iter);
    ::decode(splay_width, iter);
    ::decode(pool_id, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  bufferlist stored_orderbl;
  int r = cls_cxx_map_get_val(hctx, HEADER_KEY_ORDER, &stored_orderbl);
  if (r != -ENOENT) {
    CLS_ERR("journal already exists");
    return -EEXIST;
  }

  r = write_key(hctx, HEADER_KEY_ORDER, order);
  if (r < 0) {
    return r;
  }

  r = write_key(hctx, HEADER_KEY_SPLAY_WIDTH, splay_width);
  if (r < 0) {
    return r;
  }

  r = write_key(hctx, HEADER_KEY_POOL_ID, pool_id);
  if (r < 0) {
    return r;
  }

  uint64_t object_set = 0;
  r = write_key(hctx, HEADER_KEY_ACTIVE_SET, object_set);
  if (r < 0) {
    return r;
  }

  r = write_key(hctx, HEADER_KEY_MINIMUM_SET, object_set);
  if (r < 0) {
    return r;
  }

  uint64_t tag_id = 0;
  r = write_key(hctx, HEADER_KEY_NEXT_TAG_TID, tag_id);
  if (r < 0) {
    return r;
  }

  r = write_key(hctx, HEADER_KEY_NEXT_TAG_CLASS, tag_id);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * order (uint8_t)
 * @returns 0 on success, negative error code on failure
 */
int journal_get_order(cls_method_context_t hctx, bufferlist *in,
                      bufferlist *out) {
  uint8_t order;
  int r = read_key(hctx, HEADER_KEY_ORDER, &order);
  if (r < 0) {
    return r;
  }

  ::encode(order, *out);
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * order (uint8_t)
 * @returns 0 on success, negative error code on failure
 */
int journal_get_splay_width(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  uint8_t splay_width;
  int r = read_key(hctx, HEADER_KEY_SPLAY_WIDTH, &splay_width);
  if (r < 0) {
    return r;
  }

  ::encode(splay_width, *out);
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * pool_id (int64_t)
 * @returns 0 on success, negative error code on failure
 */
int journal_get_pool_id(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  int64_t pool_id;
  int r = read_key(hctx, HEADER_KEY_POOL_ID, &pool_id);
  if (r < 0) {
    return r;
  }

  ::encode(pool_id, *out);
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * object set (uint64_t)
 * @returns 0 on success, negative error code on failure
 */
int journal_get_minimum_set(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  uint64_t minimum_set;
  int r = read_key(hctx, HEADER_KEY_MINIMUM_SET, &minimum_set);
  if (r < 0) {
    return r;
  }

  ::encode(minimum_set, *out);
  return 0;
}

/**
 * Input:
 * @param object set (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_set_minimum_set(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  uint64_t object_set;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(object_set, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  uint64_t current_active_set;
  int r = read_key(hctx, HEADER_KEY_ACTIVE_SET, &current_active_set);
  if (r < 0) {
    return r;
  }

  if (current_active_set < object_set) {
    CLS_ERR("active object set earlier than minimum: %" PRIu64
            " < %" PRIu64, current_active_set, object_set);
    return -EINVAL;
  }

  uint64_t current_minimum_set;
  r = read_key(hctx, HEADER_KEY_MINIMUM_SET, &current_minimum_set);
  if (r < 0) {
    return r;
  }

  if (object_set == current_minimum_set) {
    return 0;
  } else if (object_set < current_minimum_set) {
    CLS_ERR("object number earlier than current object: %" PRIu64 " < %" PRIu64,
            object_set, current_minimum_set);
    return -ESTALE;
  }

  r = write_key(hctx, HEADER_KEY_MINIMUM_SET, object_set);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * object set (uint64_t)
 * @returns 0 on success, negative error code on failure
 */
int journal_get_active_set(cls_method_context_t hctx, bufferlist *in,
                           bufferlist *out) {
  uint64_t active_set;
  int r = read_key(hctx, HEADER_KEY_ACTIVE_SET, &active_set);
  if (r < 0) {
    return r;
  }

  ::encode(active_set, *out);
  return 0;
}

/**
 * Input:
 * @param object set (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_set_active_set(cls_method_context_t hctx, bufferlist *in,
                           bufferlist *out) {
  uint64_t object_set;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(object_set, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  uint64_t current_minimum_set;
  int r = read_key(hctx, HEADER_KEY_MINIMUM_SET, &current_minimum_set);
  if (r < 0) {
    return r;
  }

  if (current_minimum_set > object_set) {
    CLS_ERR("minimum object set later than active: %" PRIu64
            " > %" PRIu64, current_minimum_set, object_set);
    return -EINVAL;
  }

  uint64_t current_active_set;
  r = read_key(hctx, HEADER_KEY_ACTIVE_SET, &current_active_set);
  if (r < 0) {
    return r;
  }

  if (object_set == current_active_set) {
    return 0;
  } else if (object_set < current_active_set) {
    CLS_ERR("object number earlier than current object: %" PRIu64 " < %" PRIu64,
            object_set, current_active_set);
    return -ESTALE;
  }

  r = write_key(hctx, HEADER_KEY_ACTIVE_SET, object_set);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param id (string) - unique client id
 *
 * Output:
 * cls::journal::Client
 * @returns 0 on success, negative error code on failure
 */
int journal_get_client(cls_method_context_t hctx, bufferlist *in,
                       bufferlist *out) {
  std::string id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key(key_from_client_id(id));
  cls::journal::Client client;
  int r = read_key(hctx, key, &client);
  if (r < 0) {
    return r;
  }

  ::encode(client, *out);
  return 0;
}

/**
 * Input:
 * @param id (string) - unique client id
 * @param data (bufferlist) - opaque data associated to client
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_client_register(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  std::string id;
  bufferlist data;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
    ::decode(data, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key(key_from_client_id(id));
  bufferlist stored_clientbl;
  int r = cls_cxx_map_get_val(hctx, key, &stored_clientbl);
  if (r != -ENOENT) {
    CLS_ERR("duplicate client id: %s", id.c_str());
    return -EEXIST;
  }

  cls::journal::Client client(id, data);
  key = key_from_client_id(id);
  r = write_key(hctx, key, client);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param id (string) - unique client id
 * @param data (bufferlist) - opaque data associated to client
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_client_update_data(cls_method_context_t hctx, bufferlist *in,
                               bufferlist *out) {
  std::string id;
  bufferlist data;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
    ::decode(data, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key(key_from_client_id(id));
  cls::journal::Client client;
  int r = read_key(hctx, key, &client);
  if (r < 0) {
    return r;
  }

  client.data = data;
  r = write_key(hctx, key, client);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param id (string) - unique client id
 * @param state (uint8_t) - client state
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_client_update_state(cls_method_context_t hctx, bufferlist *in,
                                bufferlist *out) {
  std::string id;
  cls::journal::ClientState state;
  bufferlist data;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
    uint8_t state_raw;
    ::decode(state_raw, iter);
    state = static_cast<cls::journal::ClientState>(state_raw);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key(key_from_client_id(id));
  cls::journal::Client client;
  int r = read_key(hctx, key, &client);
  if (r < 0) {
    return r;
  }

  client.state = state;
  r = write_key(hctx, key, client);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param id (string) - unique client id
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_client_unregister(cls_method_context_t hctx, bufferlist *in,
                              bufferlist *out) {
  std::string id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key(key_from_client_id(id));
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
    CLS_ERR("client is not registered: %s", id.c_str());
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, key);
  if (r < 0) {
    CLS_ERR("failed to remove omap key: %s", key.c_str());
    return r;
  }

  // prune expired tags
  r = expire_tags(hctx, &id);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param client_id (uint64_t) - unique client id
 * @param commit_position (ObjectSetPosition)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_client_commit(cls_method_context_t hctx, bufferlist *in,
                          bufferlist *out) {
  std::string id;
  cls::journal::ObjectSetPosition commit_position;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
    ::decode(commit_position, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  uint8_t splay_width;
  int r = read_key(hctx, HEADER_KEY_SPLAY_WIDTH, &splay_width);
  if (r < 0) {
    return r;
  }
  if (commit_position.object_positions.size() > splay_width) {
    CLS_ERR("too many object positions");
    return -EINVAL;
  }

  std::string key(key_from_client_id(id));
  cls::journal::Client client;
  r = read_key(hctx, key, &client);
  if (r < 0) {
    return r;
  }

  if (client.commit_position == commit_position) {
    return 0;
  }

  client.commit_position = commit_position;
  r = write_key(hctx, key, client);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param start_after (string)
 * @param max_return (uint64_t)
 *
 * Output:
 * clients (set<cls::journal::Client>) - collection of registered clients
 * @returns 0 on success, negative error code on failure
 */
int journal_client_list(cls_method_context_t hctx, bufferlist *in,
                        bufferlist *out) {
  std::string start_after;
  uint64_t max_return;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(start_after, iter);
    ::decode(max_return, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string last_read;
  if (!start_after.empty()) {
    last_read = key_from_client_id(start_after);
  }

  std::map<std::string, bufferlist> vals;
  int r = cls_cxx_map_get_vals(hctx, last_read, HEADER_KEY_CLIENT_PREFIX,
                               max_return, &vals);
  if (r < 0) {
    CLS_ERR("failed to retrieve omap values: %s", cpp_strerror(r).c_str());
    return r;
  }

  std::set<cls::journal::Client> clients;
  for (std::map<std::string, bufferlist>::iterator it = vals.begin();
       it != vals.end(); ++it) {
    try {
      bufferlist::iterator iter = it->second.begin();

      cls::journal::Client client;
      ::decode(client, iter);
      clients.insert(client);
    } catch (const buffer::error &err) {
      CLS_ERR("could not decode client '%s': %s", it->first.c_str(),
              err.what());
      return -EIO;
    }
  }

  ::encode(clients, *out);
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_get_next_tag_tid(cls_method_context_t hctx, bufferlist *in,
                             bufferlist *out) {
  uint64_t tag_tid;
  int r = read_key(hctx, HEADER_KEY_NEXT_TAG_TID, &tag_tid);
  if (r < 0) {
    return r;
  }

  ::encode(tag_tid, *out);
  return 0;
}

/**
 * Input:
 * @param tag_tid (uint64_t)
 *
 * Output:
 * cls::journal::Tag
 * @returns 0 on success, negative error code on failure
 */
int journal_get_tag(cls_method_context_t hctx, bufferlist *in,
                    bufferlist *out) {
  uint64_t tag_tid;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(tag_tid, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key(key_from_tag_tid(tag_tid));
  cls::journal::Tag tag;
  int r = read_key(hctx, key, &tag);
  if (r < 0) {
    return r;
  }

  ::encode(tag, *out);
  return 0;
}

/**
 * Input:
 * @param tag_tid (uint64_t)
 * @param tag_class (uint64_t)
 * @param data (bufferlist)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_tag_create(cls_method_context_t hctx, bufferlist *in,
                       bufferlist *out) {
  uint64_t tag_tid;
  uint64_t tag_class;
  bufferlist data;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(tag_tid, iter);
    ::decode(tag_class, iter);
    ::decode(data, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key(key_from_tag_tid(tag_tid));
  bufferlist stored_tag_bl;
  int r = cls_cxx_map_get_val(hctx, key, &stored_tag_bl);
  if (r != -ENOENT) {
    CLS_ERR("duplicate tag id: %" PRIu64, tag_tid);
    return -EEXIST;
  }

  // verify tag tid ordering
  uint64_t next_tag_tid;
  r = read_key(hctx, HEADER_KEY_NEXT_TAG_TID, &next_tag_tid);
  if (r < 0) {
    return r;
  }
  if (tag_tid != next_tag_tid) {
    CLS_LOG(5, "out-of-order tag sequence: %" PRIu64, tag_tid);
    return -ESTALE;
  }

  uint64_t next_tag_class;
  r = read_key(hctx, HEADER_KEY_NEXT_TAG_CLASS, &next_tag_class);
  if (r < 0) {
    return r;
  }

  if (tag_class == cls::journal::Tag::TAG_CLASS_NEW) {
    // allocate a new tag class
    tag_class = next_tag_class;
    r = write_key(hctx, HEADER_KEY_NEXT_TAG_CLASS, tag_class + 1);
    if (r < 0) {
      return r;
    }
  } else {
    // verify tag class range
    if (tag_class >= next_tag_class) {
      CLS_ERR("out-of-sequence tag class: %" PRIu64, tag_class);
      return -EINVAL;
    }
  }

  // prune expired tags
  r = expire_tags(hctx, nullptr);
  if (r < 0) {
    return r;
  }

  // update tag tid sequence
  r = write_key(hctx, HEADER_KEY_NEXT_TAG_TID, tag_tid + 1);
  if (r < 0) {
    return r;
  }

  // write tag structure
  cls::journal::Tag tag(tag_tid, tag_class, data);
  key = key_from_tag_tid(tag_tid);
  r = write_key(hctx, key, tag);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param start_after_tag_tid (uint64_t) - first tag tid
 * @param max_return (uint64_t) - max tags to return
 * @param client_id (std::string) - client id filter
 * @param tag_class (boost::optional<uint64_t> - optional tag class filter
 *
 * Output:
 * std::set<cls::journal::Tag> - collection of tags
 * @returns 0 on success, negative error code on failure
 */
int journal_tag_list(cls_method_context_t hctx, bufferlist *in,
                     bufferlist *out) {
  uint64_t start_after_tag_tid;
  uint64_t max_return;
  std::string client_id;
  boost::optional<uint64_t> tag_class(0);

  // handle compiler false positive about use-before-init
  tag_class = boost::none;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(start_after_tag_tid, iter);
    ::decode(max_return, iter);
    ::decode(client_id, iter);
    ::decode(tag_class, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  // calculate the minimum tag within client's commit position
  uint64_t minimum_tag_tid = std::numeric_limits<uint64_t>::max();
  cls::journal::Client client;
  int r = read_key(hctx, key_from_client_id(client_id), &client);
  if (r < 0) {
    return r;
  }

  for (auto object_position : client.commit_position.object_positions) {
    minimum_tag_tid = MIN(minimum_tag_tid, object_position.tag_tid);
  }

  // compute minimum tags in use per-class
  std::set<cls::journal::Tag> tags;
  std::map<uint64_t, uint64_t> minimum_tag_class_to_tids;
  typedef enum { TAG_PASS_CALCULATE_MINIMUMS,
                 TAG_PASS_LIST,
                 TAG_PASS_DONE } TagPass;
  int tag_pass = (minimum_tag_tid == std::numeric_limits<uint64_t>::max() ?
    TAG_PASS_LIST : TAG_PASS_CALCULATE_MINIMUMS);
  std::string last_read = HEADER_KEY_TAG_PREFIX;
  do {
    std::map<std::string, bufferlist> vals;
    r = cls_cxx_map_get_vals(hctx, last_read, HEADER_KEY_TAG_PREFIX,
                             MAX_KEYS_READ, &vals);
    if (r < 0 && r != -ENOENT) {
      CLS_ERR("failed to retrieve tags: %s", cpp_strerror(r).c_str());
      return r;
    }

    for (auto &val : vals) {
      cls::journal::Tag tag;
      bufferlist::iterator iter = val.second.begin();
      try {
        ::decode(tag, iter);
      } catch (const buffer::error &err) {
        CLS_ERR("error decoding tag: %s", val.first.c_str());
        return -EIO;
      }

      if (tag_pass == TAG_PASS_CALCULATE_MINIMUMS) {
        minimum_tag_class_to_tids[tag.tag_class] = tag.tid;

        // completed calculation of tag class minimums
        if (tag.tid >= minimum_tag_tid) {
          vals.clear();
          break;
        }
      } else if (tag_pass == TAG_PASS_LIST) {
        if (start_after_tag_tid != 0 && tag.tid <= start_after_tag_tid) {
          continue;
        }

        if (tag.tid >= minimum_tag_class_to_tids[tag.tag_class] &&
            (!tag_class || *tag_class == tag.tag_class)) {
          tags.insert(tag);
        }
        if (tags.size() >= max_return) {
          tag_pass = TAG_PASS_DONE;
        }
      }
    }

    if (tag_pass != TAG_PASS_DONE && vals.size() < MAX_KEYS_READ) {
      last_read = HEADER_KEY_TAG_PREFIX;
      ++tag_pass;
    } else if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  } while (tag_pass != TAG_PASS_DONE);

  ::encode(tags, *out);
  return 0;
}

/**
 * Input:
 * @param soft_max_size (uint64_t)
 *
 * Output:
 * @returns 0 if object size less than max, negative error code otherwise
 */
int journal_object_guard_append(cls_method_context_t hctx, bufferlist *in,
                                bufferlist *out) {
  uint64_t soft_max_size;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(soft_max_size, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  uint64_t size;
  time_t mtime;
  int r = cls_cxx_stat(hctx, &size, &mtime);
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    CLS_ERR("failed to stat object: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (size >= soft_max_size) {
    CLS_LOG(5, "journal object full: %" PRIu64 " >= %" PRIu64,
            size, soft_max_size);
    return -EOVERFLOW;
  }
  return 0;
}

#if __GNUC__ >= 4
  #define CEPH_CLS_API    __attribute__ ((visibility ("default")))
#else
  #define CEPH_CLS_API
#endif

void CEPH_CLS_API __cls_init()
{
  CLS_LOG(20, "Loaded journal class!");

  cls_register("journal", &h_class);

  /// methods for journal.$journal_id objects
  cls_register_cxx_method(h_class, "create",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_create, &h_journal_create);
  cls_register_cxx_method(h_class, "get_order",
                          CLS_METHOD_RD,
                          journal_get_order, &h_journal_get_order);
  cls_register_cxx_method(h_class, "get_splay_width",
                          CLS_METHOD_RD,
                          journal_get_splay_width, &h_journal_get_splay_width);
  cls_register_cxx_method(h_class, "get_pool_id",
                          CLS_METHOD_RD,
                          journal_get_pool_id, &h_journal_get_pool_id);
  cls_register_cxx_method(h_class, "get_minimum_set",
                          CLS_METHOD_RD,
                          journal_get_minimum_set,
                          &h_journal_get_minimum_set);
  cls_register_cxx_method(h_class, "set_minimum_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_set_minimum_set,
                          &h_journal_set_minimum_set);
  cls_register_cxx_method(h_class, "get_active_set",
                          CLS_METHOD_RD,
                          journal_get_active_set,
                          &h_journal_get_active_set);
  cls_register_cxx_method(h_class, "set_active_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_set_active_set,
                          &h_journal_set_active_set);

  cls_register_cxx_method(h_class, "get_client",
                          CLS_METHOD_RD,
                          journal_get_client, &h_journal_get_client);
  cls_register_cxx_method(h_class, "client_register",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_client_register, &h_journal_client_register);
  cls_register_cxx_method(h_class, "client_update_data",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_client_update_data,
                          &h_journal_client_update_data);
  cls_register_cxx_method(h_class, "client_update_state",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_client_update_state,
                          &h_journal_client_update_state);
  cls_register_cxx_method(h_class, "client_unregister",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_client_unregister,
                          &h_journal_client_unregister);
  cls_register_cxx_method(h_class, "client_commit",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_client_commit, &h_journal_client_commit);
  cls_register_cxx_method(h_class, "client_list",
                          CLS_METHOD_RD,
                          journal_client_list, &h_journal_client_list);

  cls_register_cxx_method(h_class, "get_next_tag_tid",
                          CLS_METHOD_RD,
                          journal_get_next_tag_tid,
                          &h_journal_get_next_tag_tid);
  cls_register_cxx_method(h_class, "get_tag",
                          CLS_METHOD_RD,
                          journal_get_tag, &h_journal_get_tag);
  cls_register_cxx_method(h_class, "tag_create",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_tag_create, &h_journal_tag_create);
  cls_register_cxx_method(h_class, "tag_list",
                          CLS_METHOD_RD,
                          journal_tag_list, &h_journal_tag_list);

  /// methods for journal_data.$journal_id.$object_id objects
  cls_register_cxx_method(h_class, "guard_append",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_object_guard_append,
                          &h_journal_object_guard_append);
}
