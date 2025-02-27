import {
  CLOUD_TIER,
  Target,
  TierTarget,
  ZoneGroup,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';

export class BucketTieringUtils {
  static filterAndMapTierTargets(zonegroupData: ZoneGroupDetails) {
    return zonegroupData.zonegroups.flatMap((zoneGroup: ZoneGroup) =>
      zoneGroup.placement_targets
        .filter((target: Target) => target.tier_targets)
        .flatMap((target: Target) =>
          target.tier_targets
            .filter((tierTarget: TierTarget) => tierTarget.val.tier_type === CLOUD_TIER)
            .map((tierTarget: TierTarget) => {
              return this.getTierTargets(tierTarget, zoneGroup.name, target.name);
            })
        )
    );
  }

  private static getTierTargets(tierTarget: TierTarget, zoneGroup: string, targetName: string) {
    if (tierTarget.val.tier_type !== CLOUD_TIER) return null;
    return {
      zonegroup_name: zoneGroup,
      placement_target: targetName,
      storage_class: tierTarget.val.storage_class,
      ...tierTarget.val.s3
    };
  }
}
