#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include <curl/curl.h>

#include "fcgiapp.h"

#include "rgw_common.h"
#include "rgw_access.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_os.h"
#include "rgw_log.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "common/BackTrace.h"

using namespace std;

static sighandler_t sighandler_segv;
static sighandler_t sighandler_usr1;
static sighandler_t sighandler_alrm;

/*
 * ?print out the C++ errors to log in case it fails
 */
static void sigsegv_handler(int signum)
{
  BackTrace bt(0);
  bt.print(cerr);

  signal(signum, sighandler_segv);
}

static void godown_handler(int signum)
{
  FCGX_ShutdownPending();
  signal(signum, sighandler_usr1);
  alarm(5);
}

static void godown_alarm(int signum)
{
  _exit(0);
}

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int main(int argc, char *argv[])
{
  struct req_state s;
  struct fcgx_state fcgx;

  curl_global_init(CURL_GLOBAL_ALL);

  if (!RGWAccess::init_storage_provider("rados", argc, argv)) {
    cerr << "Couldn't init storage provider (RADOS)" << endl;
    return 5; //EIO
  }

  sighandler_segv = signal(SIGSEGV, sigsegv_handler);
  sighandler_usr1 = signal(SIGUSR1, godown_handler);
  sighandler_alrm = signal(SIGALRM, godown_alarm);

  while (FCGX_Accept(&fcgx.in, &fcgx.out, &fcgx.err, &fcgx.envp) >= 0) 
  {
    RGWHandler *handler = RGWHandler_REST::init_handler(&s, &fcgx);
    RGWOp *op;
    
    int ret = read_acls(&s);
    if (ret < 0) {
      switch (ret) {
      case -ENOENT:
        break;
      default:
        RGW_LOG(10) << "could not read acls" << " ret=" << ret << endl;
        goto done;
      }
    }
    if (!handler->authorize(&s)) {
      RGW_LOG(10) << "failed to authorize request" << endl;
      abort_early(&s, -EPERM);
      goto done;
    }

    ret = handler->read_permissions();
    if (ret < 0) {
      abort_early(&s, ret);
      goto done;
    }
    if (s.expect_cont)
      dump_continue(&s);

    op = handler->get_op();
    if (op) {
      op->execute();
    }
done:
    rgw_log_op(&s);
  }
  return 0;
}

