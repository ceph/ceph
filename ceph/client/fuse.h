
/* ceph_fuse_main
 * - start up fuse glue, attached to Client* cl.
 * - argc, argv should include a mount point, and 
 *   any weird fuse options you want.  by default,
 *   we will put fuse in the foreground so that it
 *   won't fork and we can see stdout.
 */
int ceph_fuse_main(Client *cl, int argc, char *argv[]);
