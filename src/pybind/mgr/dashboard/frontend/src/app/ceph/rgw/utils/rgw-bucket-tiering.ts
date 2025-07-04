import {
  Target,
  TierTarget,
  TIER_TYPE,
  ZoneGroup,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';

export class BucketTieringUtils {
  static filterAndMapTierTargets(zonegroupData: ZoneGroupDetails) {
    return zonegroupData.zonegroups.flatMap((zoneGroup: ZoneGroup) =>
      zoneGroup.placement_targets.flatMap((target: Target) => {
        const storage_class = new Set<string>(
          (target.tier_targets || []).map((tier_target: TierTarget) => tier_target.key)
        );
        const tierTargetDetails = (target.tier_targets || []).map((tierTarget: TierTarget) =>
          this.getTierTargets(tierTarget, zoneGroup.name, target.name)
        );
        const localStorageClasses = (target.storage_classes || [])
          .filter((storageClass) => storageClass !== 'STANDARD' && !storage_class.has(storageClass))
          .map((storageClass) => ({
            zonegroup_name: zoneGroup.name,
            placement_target: target.name,
            storage_class: storageClass,
            tier_type: TIER_TYPE.LOCAL
          }));

        return [...tierTargetDetails, ...localStorageClasses];
      })
    );
  }

  private static getTierTargets(tierTarget: TierTarget, zoneGroup: string, targetName: string) {
    const val = tierTarget.val;

    if (tierTarget.val.tier_type === TIER_TYPE.GLACIER) {
      return {
        zonegroup_name: zoneGroup,
        placement_target: targetName,
        storage_class: tierTarget.val.storage_class,
        retain_head_object: tierTarget.val.retain_head_object,
        allow_read_through: tierTarget.val.allow_read_through,
        glacier_restore_days: tierTarget.val['s3-glacier'].glacier_restore_days,
        glacier_restore_tier_type: tierTarget.val['s3-glacier'].glacier_restore_tier_type,
        restore_storage_class: tierTarget.val.restore_storage_class,
        read_through_restore_days: tierTarget.val.read_through_restore_days,
        tier_type: tierTarget.val.tier_type,
        region: tierTarget.val.s3.region,
        endpoint: tierTarget.val.s3.endpoint,
        target_path: tierTarget.val.s3.target_path,
        access_key: tierTarget.val.s3.access_key,
        secret: tierTarget.val.s3.secret,
        multipart_min_part_size: tierTarget.val.s3.multipart_min_part_size,
        multipart_sync_threshold: tierTarget.val.s3.multipart_sync_threshold,
        host_style: tierTarget.val.s3.host_style,
        ...tierTarget.val['s3-glacier']
      };
    } else if (val.tier_type === TIER_TYPE.CLOUD_TIER) {
      return {
        zonegroup_name: zoneGroup,
        placement_target: targetName,
        storage_class: val.storage_class,
        retain_head_object: val.retain_head_object,
        allow_read_through: val.allow_read_through,
        tier_type: val.tier_type,
        ...val.s3
      };
    } else {
      return {
        zonegroup_name: zoneGroup,
        placement_target: targetName,
        storage_class: val.storage_class,
        tier_type: TIER_TYPE.LOCAL
      };
    }
  }
}
