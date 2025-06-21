import {
  Target,
  TierTarget,
  TierType,
  ZoneGroup,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';

export class BucketTieringUtils {
  static filterAndMapTierTargets(zonegroupData: ZoneGroupDetails) {
    return zonegroupData.zonegroups.flatMap((zoneGroup: ZoneGroup) =>
      zoneGroup.placement_targets
        .filter((target: Target) => target.tier_targets)
        .flatMap((target: Target) =>
          target.tier_targets.map((tierTarget: TierTarget) => {
            return this.getTierTargets(tierTarget, zoneGroup.name, target.name);
          })
        )
    );
  }

  private static getTierTargets(tierTarget: TierTarget, zoneGroup: string, targetName: string) {
    if (tierTarget.val.tier_type === TierType.GLACIER) {
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
        ...tierTarget.val.s3
      };
    } else if (tierTarget.val.tier_type === TierType.CLOUD_TIER) {
      return {
        zonegroup_name: zoneGroup,
        placement_target: targetName,
        storage_class: tierTarget.val.storage_class,
        retain_head_object: tierTarget.val.retain_head_object,
        allow_read_through: tierTarget.val.allow_read_through,
        tier_type: tierTarget.val.tier_type,
        ...tierTarget.val.s3
      };
    } else {
      return {
        zonegroup_name: zoneGroup,
        placement_target: targetName,
        storage_class: tierTarget.val.storage_class
      };
    }
  }
}
