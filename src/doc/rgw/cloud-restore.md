# cloud-restore

## Introduction

[`cloud-transition`](https://docs.ceph.com/en/latest/radosgw/cloud-transition) feature enables data transition to a remote cloud service as part of Lifecycle Configuration via Storage Classes. However the transition is unidirectional; data cannot be transitioned back from the remote zone.

The `cloud-restore` feature enables restoration of those transitioned objects from the remote cloud S3 endpoints back into RGW.

The objects can be restored either by using S3 `restore-object` CLI or via `read-through`. The restored copies can be either temporary or permanent.

## S3 restore-object CLI

The goal here is to implement minimal functionality of [`S3RestoreObject`](https://docs.aws.amazon.com/cli/latest/reference/s3api/restore-object.html) API so that users can restore the cloud transitioned objects.

```sh
aws s3api restore-object \
                    --bucket <value> \
                    --key <value>  ( can be object name or * for Bulk restore) \
                    [--version-id <value>] \
                    --restore-request (structure) {
                     // for temporary restore
                        { "Days": integer, }  
                        // if Days not provided, it will be considered as permanent copy
                    }
```

This CLI may be extended in future to include custom parameters (like target-bucket/storage-class etc) specific to RGW.

## read-through

As per the cloud-transition feature functionality, the cloud-transitioned objects cannot be read. `GET` on those objects fails with ‘InvalidObjectState’ error.

But using this restore feature, transitioned objects can be restored and read. New tier-config options `allow_read_through` and `read_through_restore_days` are added for the same. Only when `allow_read_through` is enabled, `GET` on the transitioned objects will restore the objects from the S3 endpoint.

Note: The object copy restored via `readthrough` is temporary and is retained only for the duration of `read_through_restore_days`.

## Design

* Similar to cloud-transition feature, this feature currently works for **only s3 compatible cloud endpoint**.
* This feature works for only **cloud-transitioned objects**. In order to validate this, `retain_head_object` option should be set to true so that the object’s `HEAD` object can be verified before restoring the object.

* **Request flow:**
  * Once the `HEAD` object is verified, its cloudtier storage class config details are fetched.
Note: Incase the cloudtier storage-class is deleted/updated, the object may not be restored.
  * RestoreStatus for the `HEAD` object is marked `RestoreAlreadyInProgress`
  * Object Restore is done asynchronously by issuing either S3 `GET` or S3 `RESTORE` request to the remote endpoint.
  * Once the object is restored, RestoreStaus is updated as `CloudRestored` and RestoreType is set to either `Temporary` or `Permanent`.
  * Incase the operation fails, RestoreStatus is marked as `RestoreFailed`.

* **New attrs:** Below are the new attrs being added
  * `user.rgw.restore-status`: <Restore operation Status>
  * `user.rgw.restore-type`: <Type of Restore>
  * `user.rgw.restored-at`: <Restoration Time>
  * `user.rgw.restore-expiry-date`: <Expiration time incase of temporary copies>
  * `user.rgw.cloudtier_storage_class`: <CloudTier storage class used in case of temporarily restored copies>

```cpp
        enum RGWRestoreStatus : uint8_t {
          None  = 0,
          RestoreAlreadyInProgress = 1,
          CloudRestored = 2,
          RestoreFailed = 3
        };
        enum class RGWRestoreType : uint8_t {
          None = 0,
          Temporary = 1,
          Permanent = 2
        };
```

* **Response:**
* `S3 restore-object CLI`  returns SUCCESS - either the 200 OK or 202 Accepted status code.
  * If the object is not previously restored, then RGW returns 202 Accepted in the response.
  * If the object is previously restored, RGW returns 200 OK in the response.
    * Special errors:
        Code: RestoreAlreadyInProgress ( Cause: Object restore is already in progress.)
        Code: ObjectNotFound (if Object is not found in cloud endpoint)
        Code: I/O error (for any other I/O errors during restore)
* `GET request` continues to return an  ‘InvalidObjectState’ error till the object is successfully restored.
  * S3 head-object can be used to verify if the restore is still in progress.
  * Once the object is restored, GET will return the object data.

* **StorageClass**: By default, the objects are restored to `STANDARD` storage class. However, as per [AWS S3 Restore](https://docs.aws.amazon.com/cli/latest/reference/s3api/restore-object.html) the storage-class remains the same for restored objects. Hence for the temporary copies, the `x-amz-storage-class` returned contains original cloudtier storage-class.
  * Note: A new tier-config option may be added to select the storage-class to restore the objects to.

* **mtime**: If the restored object is temporary, object is still marked `RGWObj::CloudTiered`  and mtime is not changed i.e, still set to transition time. But in case the object is permanent copy, it is marked `RGWObj::Main` and mtime is updated to the restore time (now()).

* **Lifecycle**:
  * `Temporary` copies are not subjected to any further transition to the cloud. However (as is the case with cloud-transitioned objects) they can be deleted via regular LC expiration rules or via external S3 Delete request.
  * `Permanent` copies are treated as any regular objects and are subjected to any LC rules applicable.

* **Replication**:  The restored objects (both temporary and permanent) are also replicated like regular objects and will be deleted across the zones post expiration.

* **VersionedObjects** : In case of versioning, if any object is cloud-transitioned, it would have been non-current. Post restore too, the same non-current object will be updated with the downloaded data and its HEAD object will be updated accordingly as the case with regular objects.

* **Temporary Object Expiry**: This is done via Object Expirer
  * When the object is restored as temporary, `user.rgw.expiry-date` is set accordingly and `delete_at` attr is also updated with the same value.
  * This object is then added to the list used by `ObjectExpirer`.
  * `LC` worker thread is used to scan through that list and post expiry, resets the objects back to cloud-transitioned state i.e,
    * HEAD object with size=0
    * new attrs removed
    * `delete_at` reset
  * Note: A new RGW option `rgw_restore_debug_interval` is added, which when set will be considered as `Days` value (similar to `rgw_lc_debug_interval`).

* **FAILED Restore**: In case the restore operation fails,
  * The HEAD object will be updated accordingly.. i.e, Storage-class is reset to the original cloud-tier storage class
  * All the new attrs added will be removed , except for `user.rgw.restore-status` which will be updated as `RestoreFailed`

* **Check Restore Progress**: Users can issue S3 `head-object` request to check if the restore is done or still in progress for any object.

* **RGW down/restarts** - Since the restore operation is asynchronous, we need to keep track of the objects being restored. In case RGW is down/restarts, this data will be used to retrigger on-going restore requests or do appropriate cleanup for the failed requests.

* **Compression** - If the placement-target to which the objects are being restored to has compression enabled, the data will be compressed accordingly (bug2294512)

* **Encryption** - If the restored object is encrypted, the old sse-related xattrs/keys from the HEAD stub will be copied back into object metadata (bug2294512)

* **Delete cloud object post restore** - Once the object is successfully restored, the object at the remote endpoint is still retained. However we could choose to delete it for permanent restored copies by adding new tier-config option.

## Future work

* **Bulk Restore**: In the case of BulkRestore, some of the objects may not be restored. User needs to manually cross-check the objects to check the objects restored or InProgress.

* **Admin CLIs**: Admin debug commands will be provided to start, check the status and cancel the restore operations.

* **Admin Ops**

* **Restore Notifications**
