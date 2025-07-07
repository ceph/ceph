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
    if (val.tier_type === TIER_TYPE.CLOUD_TIER) {
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
