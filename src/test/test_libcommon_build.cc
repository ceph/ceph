#include "common/common_init.h"

void global_init_set_globals(CephContext *cct)
{
}

/* This program exists to test that we can build libcommon without
 * referencing g_ceph_context
 *
 * This program will go away as soon as we actually don't use g_ceph_context in
 * more programs. Obviously, at that point, those programs will provide an
 * equivalent test.
 */
int main(int argc, char **argv)
{
  return 0;
}
