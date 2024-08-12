#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <unistd.h>
#include "include/on_exit.h"
#include "include/ceph_assert.h"

#ifndef MAP_ANONYMOUS
# ifdef MAP_ANON
#  define MAP_ANONYMOUS MAP_ANON
# else
// cppcheck-suppress preprocessorErrorDirective
#  error "Don't know how to create anonymous mmap"
# endif
#endif

static int func_scope_val;

static void add(void *incp)
{
  func_scope_val += *((int*)incp);
}

static void func_scope(void)
{
  OnExitManager mgr;

  static int inc_1 = 5;
  mgr.add_callback(add, &inc_1);

  static int inc_2 = 3;
  mgr.add_callback(add, &inc_2);
}

// shared between processes
static int *shared_val;

#define MAIN_SCOPE_VAL 0x1111111
static OnExitManager main_scope_mgr;
static void main_scope_cb(void *val)
{
  *shared_val = *((int*)val);
}

#define EXIT_FUNC_VAL 0x22222222
static OnExitManager exit_func_mgr;
static void exit_func_cb(void *val)
{
  *shared_val = *((int*)val);
}

static void call_exit()
{
  exit(3);
}

int main(int argc, char **argv)
{
  // test basic function scope behavior
  ceph_assert(func_scope_val == 0);
  func_scope();
  ceph_assert(func_scope_val == 8);

  // shared mem for exit tests
  shared_val = (int*)mmap(NULL, sizeof(int),
      PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
  ceph_assert(shared_val != MAP_FAILED);

  // test normal exit returning from main
  *shared_val = 0;
  int pid = fork();
  ceph_assert(pid >= 0);
  if (pid) {
    int status;
    int ret = waitpid(pid, &status, 0);
    ceph_assert(ret == pid); // should be our child
    ceph_assert(status == 0);
    ceph_assert(*shared_val == MAIN_SCOPE_VAL);
  } else {
    // child adds a callback to the static scope callback manager and then
    // exits by returning from main. The parent checks the value after the
    // child exits via the memory map.
    ceph_assert(*shared_val == 0);
    static int new_val = MAIN_SCOPE_VAL;
    main_scope_mgr.add_callback(main_scope_cb, &new_val);
    return 0;
  }

  // test exit via exit()
  *shared_val = 0;
  pid = fork();
  ceph_assert(pid >= 0);
  if (pid) {
    int status;
    int ret = waitpid(pid, &status, 0);
    ceph_assert(ret == pid); // should be our child
    ceph_assert(WEXITSTATUS(status) == 3);
    ceph_assert(*shared_val == EXIT_FUNC_VAL);
  } else {
    // child adds a callback to the static scope callback manager and then
    // exits via exit().
    ceph_assert(*shared_val == 0);
    static int new_val = EXIT_FUNC_VAL;
    exit_func_mgr.add_callback(exit_func_cb, &new_val);
    call_exit();
    ceph_abort();
  }

  return 0;
}
