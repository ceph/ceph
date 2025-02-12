export const RBDActionHelpers = {
  moveToTrash: $localize`Move an image to the trash. Images, even ones actively in-use by clones, can be moved to the trash and deleted at a later time.`,
  delete: $localize`Delete an rbd image (including all data blocks). If the image has snapshots, this fails and nothing is deleted.`,
  copy: $localize`Copy the content of a source image into the newly created destination image`,
  flatten: $localize`If the image is a clone, copy all shared blocks from the parent snapshot and make the child independent of the parent, severing the link between parent snap and child. `,
  enableMirroring: $localize`Mirroring needs to be enabled on the image to perform this action`,
  clonedSnapshot: $localize`This RBD has cloned snapshots. Please delete related RBDs before deleting this RBD`,
  secondayImageDelete: $localize`The image cannot be deleted as it is secondary`,
  primaryImageResync: $localize`Primary RBD images cannot be resynced`,
  invalidNameDisable: $localize`This RBD image has an invalid name and can't be managed by ceph.`,
  removingStatus: $localize`Action not possible for an RBD in status 'Removing'`,
  journalTooltipText: $localize`'Ensures reliable replication by logging changes before updating the image, but doubles write time, impacting performance. Not recommended for high-speed data processing tasks.`,
  snapshotTooltipText: $localize`This mode replicates RBD images between clusters using snapshots, efficiently copying data changes but requiring complete delta syncing during failover. Ideal for less demanding tasks due to its less granular approach compared to journaling.`
};
