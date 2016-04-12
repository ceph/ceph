// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "KineticStore.h"
#include "common/ceph_crypto.h"

#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <errno.h>
using std::string;
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_kinetic

int KineticStore::init()
{
  // init defaults.  caller can override these if they want
  // prior to calling open.
  host = cct->_conf->kinetic_host;
  port = cct->_conf->kinetic_port;
  user_id = cct->_conf->kinetic_user_id;
  hmac_key = cct->_conf->kinetic_hmac_key;
  use_ssl = cct->_conf->kinetic_use_ssl;
  return 0;
}

int KineticStore::_test_init(CephContext *c)
{
  kinetic::KineticConnectionFactory conn_factory =
    kinetic::NewKineticConnectionFactory();

  kinetic::ConnectionOptions options;
  options.host = cct->_conf->kinetic_host;
  options.port = cct->_conf->kinetic_port;
  options.user_id = cct->_conf->kinetic_user_id;
  options.hmac_key = cct->_conf->kinetic_hmac_key;
  options.use_ssl = cct->_conf->kinetic_use_ssl;

  kinetic::Status status = conn_factory.NewThreadsafeBlockingConnection(options, kinetic_conn, 10);
  kinetic_conn.reset();
  if (!status.ok())
    derr << __func__ << "Unable to connect to kinetic store " << options.host
         << ":" << options.port << " : " << status.ToString() << dendl;
  return status.ok() ? 0 : -EIO;
}

int KineticStore::do_open(ostream &out, bool create_if_missing)
{
  kinetic::KineticConnectionFactory conn_factory =
    kinetic::NewKineticConnectionFactory();
  kinetic::ConnectionOptions options;
  options.host = host;
  options.port = port;
  options.user_id = user_id;
  options.hmac_key = hmac_key;
  options.use_ssl = use_ssl;
  kinetic::Status status = conn_factory.NewThreadsafeBlockingConnection(options, kinetic_conn, 10);
  if (!status.ok()) {
    derr << "Unable to connect to kinetic store " << host << ":" << port
	 << " : " << status.ToString() << dendl;
    return -EINVAL;
  }

  PerfCountersBuilder plb(g_ceph_context, "kinetic", l_kinetic_first, l_kinetic_last);
  plb.add_u64_counter(l_kinetic_gets, "kinetic_get", "Gets");
  plb.add_u64_counter(l_kinetic_txns, "kinetic_transaction", "Transactions");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  return 0;
}

KineticStore::KineticStore(CephContext *c) :
  cct(c),
  logger(NULL)
{
  host = c->_conf->kinetic_host;
  port = c->_conf->kinetic_port;
  user_id = c->_conf->kinetic_user_id;
  hmac_key = c->_conf->kinetic_hmac_key;
  use_ssl = c->_conf->kinetic_use_ssl;
}

KineticStore::~KineticStore()
{
  close();
  delete logger;
}

void KineticStore::close()
{
  kinetic_conn.reset();
  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int KineticStore::submit_transaction(KeyValueDB::Transaction t)
{
  KineticTransactionImpl * _t =
    static_cast<KineticTransactionImpl *>(t.get());

  dout(20) << "kinetic submit_transaction" << dendl;

  for (vector<KineticOp>::iterator it = _t->ops.begin();
       it != _t->ops.end(); ++it) {
    kinetic::KineticStatus status(kinetic::StatusCode::OK, "");
    if (it->type == KINETIC_OP_WRITE) {
      string data(it->data.c_str(), it->data.length());
      kinetic::KineticRecord record(data, "", "",
				    com::seagate::kinetic::client::proto::Message_Algorithm_SHA1);
      dout(30) << "kinetic before put of " << it->key << " (" << data.length() << " bytes)" << dendl;
      status = kinetic_conn->Put(it->key, "", kinetic::WriteMode::IGNORE_VERSION,
				 record);
      dout(30) << "kinetic after put of " << it->key << dendl;
    } else {
      assert(it->type == KINETIC_OP_DELETE);
      dout(30) << "kinetic before delete" << dendl;
      status = kinetic_conn->Delete(it->key, "",
				    kinetic::WriteMode::IGNORE_VERSION);
      dout(30) << "kinetic after delete" << dendl;
    }
    if (!status.ok()) {
      derr << "kinetic error submitting transaction: "
	   << status.message() << dendl;
      return -1;
    }
  }

  logger->inc(l_kinetic_txns);
  return 0;
}

int KineticStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  return submit_transaction(t);
}

void KineticStore::KineticTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  string key = combine_strings(prefix, k);
  dout(30) << "kinetic set key " << key << dendl;
  ops.push_back(KineticOp(KINETIC_OP_WRITE, key, to_set_bl));
}

void KineticStore::KineticTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  dout(30) << "kinetic rm key " << key << dendl;
  ops.push_back(KineticOp(KINETIC_OP_DELETE, key));
}

void KineticStore::KineticTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  dout(20) << "kinetic rmkeys_by_prefix " << prefix << dendl;
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    ops.push_back(KineticOp(KINETIC_OP_DELETE, key));
    dout(30) << "kinetic rm key by prefix: " << key << dendl;
  }
}

int KineticStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  dout(30) << "kinetic get prefix: " << prefix << " keys: " << keys << dendl;
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    unique_ptr<kinetic::KineticRecord> record;
    string key = combine_strings(prefix, *i);
    dout(30) << "before get key " << key << dendl;
    kinetic::KineticStatus status = kinetic_conn->Get(key, record);
    if (!status.ok())
      break;
    dout(30) << "kinetic get got key: " << key << dendl;
    out->insert(make_pair(key, to_bufferlist(*record.get())));
  }
  logger->inc(l_kinetic_gets);
  return 0;
}

string KineticStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(1);
  out.append(value);
  return out;
}

bufferlist KineticStore::to_bufferlist(const kinetic::KineticRecord &record)
{
  bufferlist bl;
  bl.append(*(record.value()));
  return bl;
}

int KineticStore::split_key(string &in, string *prefix, string *key)
{
  size_t prefix_len = 0;
  char* in_data = in.c_str();
  
  // Find separator inside Slice
  char* separator = (char*) memchr((void*)in_data, 1, in.size());
  if (separator == NULL)
     return -EINVAL;
  prefix_len = size_t(separator - in_data);
  if (prefix_len >= in.size())
    return -EINVAL;

  // Fetch prefix and/or key directly from Slice
  if (prefix)
    *prefix = string(in_data, prefix_len);
  if (key)
    *key = string(separator+1, in.size()-prefix_len-1);
  return 0;
}

KineticStore::KineticWholeSpaceIteratorImpl::KineticWholeSpaceIteratorImpl(kinetic::BlockingKineticConnection *conn) : kinetic_conn(conn),
   kinetic_status(kinetic::StatusCode::OK, "")
{
  dout(30) << "kinetic iterator constructor()" << dendl;
  const static string last_key = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
  kinetic::KeyRangeIterator it =
    kinetic_conn->IterateKeyRange("", true, last_key, true, 1024);
  while (it != kinetic::KeyRangeEnd()) {
    try {
      keys.insert(*it);
      dout(30) << "kinetic iterator added " << *it << dendl;
    } catch (std::runtime_error &e) {
      kinetic_status = kinetic::KineticStatus(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, e.what());
      return;
    }
    ++it;
  }
  keys_iter = keys.begin();
}

int KineticStore::KineticWholeSpaceIteratorImpl::seek_to_first(const string &prefix)
{
  dout(30) << "kinetic iterator seek_to_first(prefix): " << prefix << dendl;
  keys_iter = keys.lower_bound(prefix);
  return 0;
}

int KineticStore::KineticWholeSpaceIteratorImpl::seek_to_last()
{
  dout(30) << "kinetic iterator seek_to_last()" << dendl;
  keys_iter = keys.end();
  if (keys.begin() != keys_iter)
    --keys_iter;
  return 0;
}

int KineticStore::KineticWholeSpaceIteratorImpl::seek_to_last(const string &prefix)
{
  dout(30) << "kinetic iterator seek_to_last(prefix): " << prefix << dendl;
  keys_iter = keys.upper_bound(prefix + "\2");
  if (keys.begin() == keys_iter) {
    keys_iter = keys.end();
  } else {
    --keys_iter;
  }
  return 0;
}

int KineticStore::KineticWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after) {
  dout(30) << "kinetic iterator upper_bound()" << dendl;
  string bound = combine_strings(prefix, after);
  keys_iter = keys.upper_bound(bound);
  return 0;
}

int KineticStore::KineticWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to) {
  dout(30) << "kinetic iterator lower_bound()" << dendl;
  string bound = combine_strings(prefix, to);
  keys_iter = keys.lower_bound(bound);
  return 0;
}

bool KineticStore::KineticWholeSpaceIteratorImpl::valid() {
  dout(30) << "kinetic iterator valid()" << dendl;
  return keys_iter != keys.end();
}

int KineticStore::KineticWholeSpaceIteratorImpl::next() {
  dout(30) << "kinetic iterator next()" << dendl;
  if (keys_iter != keys.end()) {
      ++keys_iter;
      return 0;
  }
  return -1;
}

int KineticStore::KineticWholeSpaceIteratorImpl::prev() {
  dout(30) << "kinetic iterator prev()" << dendl;
  if (keys_iter != keys.begin()) {
      --keys_iter;
      return 0;
  }
  keys_iter = keys.end();
  return -1;
}

string KineticStore::KineticWholeSpaceIteratorImpl::key() {
  dout(30) << "kinetic iterator key()" << dendl;
  string out_key;
  split_key(*keys_iter, NULL, &out_key);
  return out_key;
}

pair<string,string> KineticStore::KineticWholeSpaceIteratorImpl::raw_key() {
  dout(30) << "kinetic iterator raw_key()" << dendl;
  string prefix, key;
  split_key(*keys_iter, &prefix, &key);
  return make_pair(prefix, key);
}

bool KineticStore::KineticWholeSpaceIteratorImpl::raw_key_is_prefixed(const string &prefix) {
  // Look for "prefix\1" right in *keys_iter without making a copy
  string key = *keys_iter;
  if ((key.size() > prefix.length()) && (key[prefix.length()] == '\1')) {
    return memcmp(key.c_str(), prefix.c_str(), prefix.length()) == 0;
  } else {
    return false;
  }
}


bufferlist KineticStore::KineticWholeSpaceIteratorImpl::value() {
  dout(30) << "kinetic iterator value()" << dendl;
  unique_ptr<kinetic::KineticRecord> record;
  kinetic_status = kinetic_conn->Get(*keys_iter, record);
  return to_bufferlist(*record.get());
}

int KineticStore::KineticWholeSpaceIteratorImpl::status() {
  dout(30) << "kinetic iterator status()" << dendl;
  return kinetic_status.ok() ? 0 : -1;
}
