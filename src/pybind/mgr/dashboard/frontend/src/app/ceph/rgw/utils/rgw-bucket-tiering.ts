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
    const tierType = val.tier_type;
    const commonProps = {
      zonegroup_name: zoneGroup,
      placement_target: targetName,
      storage_class: val.storage_class,
      tier_type: tierType
    };
    const cloudProps = {
      ...commonProps,
      retain_head_object: val.retain_head_object,
      allow_read_through: val.allow_read_through,
      restore_storage_class: val.restore_storage_class,
      read_through_restore_days: val.read_through_restore_days,
      ...val.s3
    };

    if (!tierType || tierType === TIER_TYPE.LOCAL) {
      return commonProps;
    }

    if (tierType === TIER_TYPE.GLACIER) {
      return {
        ...cloudProps,
        ...val['s3-glacier']
      };
    }
    return cloudProps;
  }
}
