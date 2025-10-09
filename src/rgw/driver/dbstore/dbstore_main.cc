#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "dbstore_mgr.h"
#include <dbstore.h>
#include <dbstore_log.h>

using namespace std;
using namespace rgw::store;
using DB = rgw::store::DB;

struct thr_args {
  DB *dbs;
  int thr_id;
};

void* process(void *arg)
{
  struct thr_args *t_args = (struct thr_args*)arg;

  DB *db = t_args->dbs;
  int thr_id = t_args->thr_id;
  int ret = -1;

  cout<<"Entered thread:"<<thr_id<<"\n";

  string user1 = "User1";
  string bucketa = "rgw";
  string objecta1 = "bugfixing";
  string objecta2 = "zipper";
  string bucketb = "gluster";
  string objectb1 = "bugfixing";
  string objectb2 = "delegations";

  string user2 = "User2";
  string bucketc = "qe";
  string objectc1 = "rhhi";
  string objectc2 = "cns";

  DBOpParams params = {};
  const DoutPrefixProvider *dpp = db->get_def_dpp();

  db->InitializeParams(dpp, &params);

  params.op.user.uinfo.display_name = user1;
  params.op.user.uinfo.user_id.tenant = "tenant";
  params.op.user.uinfo.user_id.id = user1;
  params.op.user.uinfo.suspended = 123;
  params.op.user.uinfo.max_buckets = 456;
  params.op.user.uinfo.placement_tags.push_back("tags1");
  params.op.user.uinfo.placement_tags.push_back("tags2");

  RGWAccessKey k1("id1", "key1");
  RGWAccessKey k2("id2", "key2");
  params.op.user.uinfo.access_keys.insert(make_pair("key1", k1));
  params.op.user.uinfo.access_keys.insert(make_pair("key2", k2));

  ret = db->ProcessOp(dpp, "InsertUser", &params);
  cout << "InsertUser return value: " <<  ret << "\n";

  DBOpParams params2 = {};
  params.op.user.uinfo.user_id.tenant = "tenant2";

  db->InitializeParams(dpp, &params2);
  params2.op.user.uinfo.display_name = user1;
  ret = db->ProcessOp(dpp, "GetUser", &params2);

  cout << "GetUser return value: " <<  ret << "\n";

  cout << "tenant: " << params2.op.user.uinfo.user_id.tenant << "\n";
  cout << "suspended: " << (int)params2.op.user.uinfo.suspended << "\n";

  list<string>::iterator it = params2.op.user.uinfo.placement_tags.begin();

  while (it != params2.op.user.uinfo.placement_tags.end()) {
    cout << "list = " << *it << "\n";
    it++;
  }

  map<string, RGWAccessKey>::iterator it2 = params2.op.user.uinfo.access_keys.begin();

  while (it2 != params2.op.user.uinfo.access_keys.end()) {
    cout << "keys = " << it2->first << "\n";
    RGWAccessKey k = it2->second;
    cout << "id = " << k.id << ", keys = " << k.key << "\n";
    it2++;
  }

  params.op.bucket.info.bucket.name = bucketa;
  db->ProcessOp(dpp, "InsertBucket", &params);

  params.op.user.uinfo.display_name = user2;
  params.op.user.uinfo.user_id.id = user2;
  db->ProcessOp(dpp, "InsertUser", &params);

  params.op.bucket.info.bucket.name = bucketb;
  db->ProcessOp(dpp, "InsertBucket", &params);

  db->ProcessOp(dpp, "GetUser", &params);
  db->ProcessOp(dpp, "GetBucket", &params);

  db->ListAllUsers(dpp, &params);
  db->ListAllBuckets(dpp, &params);

  params.op.bucket.info.bucket.name = bucketb;

  db->ProcessOp(dpp, "RemoveBucket", &params);

  params.op.user.uinfo.user_id.id = user2;
  db->ProcessOp(dpp, "RemoveUser", &params);

  db->ListAllUsers(dpp, &params);
  db->ListAllBuckets(dpp, &params);
  cout<<"Exiting thread:"<<thr_id<<"\n";

  return 0;
}

// This has an uncaught exception. Even if the exception is caught, the program
// would need to be terminated, so the warning is simply suppressed.
// coverity[root_function:SUPPRESS]
int main(int argc, char *argv[])
{
  string tenant = "Redhat";
  string logfile = "rgw_dbstore_bin.log";
  int loglevel = 20;

  DBStoreManager *dbsm;
  DB *dbs;
  int rc = 0, tnum = 0;
  void *res;

  pthread_attr_t attr;
  int num_thr = 2;
  pthread_t threads[num_thr];
  struct thr_args t_args[num_thr];


  cout << "loglevel  " << loglevel << "\n";
  // format: ./dbstore-bin logfile loglevel
  if (argc == 3) {
	logfile = argv[1];
	loglevel = (atoi)(argv[2]);
	cout << "loglevel set to " << loglevel << "\n";
  }

  vector<const char*> args;
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                CODE_ENVIRONMENT_DAEMON, CINIT_FLAG_NO_MON_CONFIG, 1);
  dbsm = new DBStoreManager(cct.get(), logfile, loglevel);
  dbs = dbsm->getDB(tenant, true);

  cout<<"No. of threads being created = "<<num_thr<<"\n";

  /* Initialize thread creation attributes */
  rc = pthread_attr_init(&attr);

  if (rc != 0) {
    cout<<" error in pthread_attr_init \n";
    goto out;
  }

  for (tnum = 0; tnum < num_thr; tnum++) {
    t_args[tnum].dbs = dbs;
    t_args[tnum].thr_id = tnum;
    rc = pthread_create((pthread_t*)&threads[tnum], &attr, &process,
        &t_args[tnum]);
    if (rc != 0) {
      cout<<" error in pthread_create \n";
      goto out;
    }

    cout<<"Created thread (thread-id:"<<tnum<<")\n";
  }

  /* Destroy the thread attributes object, since it is no
     longer needed */

  rc = pthread_attr_destroy(&attr);
  if (rc != 0) {
    cout<<"error in pthread_attr_destroy \n";
  }

  /* Now join with each thread, and display its returned value */

  for (tnum = 0; tnum < num_thr; tnum++) {
    rc = pthread_join(threads[tnum], &res);
    if (rc != 0) {
      cout<<"error in pthread_join \n";
    } else {
      cout<<"Joined with thread "<<tnum<<"\n";
    }
  }

out:
  dbsm->destroyAllHandles();

  return 0;
}
