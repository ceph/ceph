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
cls_method_handle_t h_journal_get_minimum_set;
cls_method_handle_t h_journal_set_minimum_set;
cls_method_handle_t h_journal_get_active_set;
cls_method_handle_t h_journal_set_active_set;
cls_method_handle_t h_journal_client_register;
cls_method_handle_t h_journal_client_unregister;
cls_method_handle_t h_journal_client_commit;
cls_method_handle_t h_journal_client_list;
cls_method_handle_t h_journal_object_guard_append;

namespace {

static const uint64_t MAX_KEYS_READ = 64;

static const std::string HEADER_KEY_ORDER         = "order";
static const std::string HEADER_KEY_SPLAY_WIDTH   = "splay_width";
static const std::string HEADER_KEY_MINIMUM_SET   = "minimum_set";
static const std::string HEADER_KEY_ACTIVE_SET    = "active_set";
static const std::string HEADER_KEY_CLIENT_PREFIX = "client_";

static void key_from_client_id(const std::string &client_id, string *key) {
  *key = HEADER_KEY_CLIENT_PREFIX + client_id;
}

template <typename T>
int read_key(cls_method_context_t hctx, const string &key, T *t) {
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
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
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(order, iter);
    ::decode(splay_width, iter);
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

  uint64_t object_set = 0;
  r = write_key(hctx, HEADER_KEY_ACTIVE_SET, object_set);
  if (r < 0) {
    return r;
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
 * @param description (string) - human-readable description of the client
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int journal_client_register(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  std::string id;
  std::string description;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
    ::decode(description, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode input parameters: %s", err.what());
    return -EINVAL;
  }

  std::string key;
  key_from_client_id(id, &key);

  bufferlist stored_clientbl;
  int r = cls_cxx_map_get_val(hctx, key, &stored_clientbl);
  if (r != -ENOENT) {
    CLS_ERR("duplicate client id: %s", id.c_str());
    return -EEXIST;
  }

  cls::journal::Client client(id, description);
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

  std::string key;
  key_from_client_id(id, &key);

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
  if (commit_position.entry_positions.size() > splay_width) {
    CLS_ERR("too many entry positions");
    return -EINVAL;
  }

  std::string key;
  key_from_client_id(id, &key);

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
  key_from_client_id(start_after, &last_read);

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
  cls_register_cxx_method(h_class, "client_register",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_client_register, &h_journal_client_register);
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

  /// methods for journal_data.$journal_id.$object_id objects
  cls_register_cxx_method(h_class, "guard_append",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          journal_object_guard_append,
                          &h_journal_object_guard_append);
}
