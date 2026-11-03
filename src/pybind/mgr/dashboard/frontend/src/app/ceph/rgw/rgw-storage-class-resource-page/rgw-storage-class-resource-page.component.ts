import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';
import { finalize } from 'rxjs/operators';

import {
  ACL,
  ALLOW_READ_THROUGH_TEXT,
  AllZonesResponse,
  GLACIER_RESTORE_DAY_TEXT,
  GLACIER_RESTORE_TIER_TYPE_TEXT,
  HOST_STYLE,
  MULTIPART_MIN_PART_TEXT,
  MULTIPART_SYNC_THRESHOLD_TEXT,
  READTHROUGH_RESTORE_DAYS_TEXT,
  RESTORE_STORAGE_CLASS_TEXT,
  RETAIN_HEAD_OBJECT_TEXT,
  StorageClassDetails,
  TARGET_ACCESS_KEY_TEXT,
  TARGET_PATH_TEXT,
  LOCATION_CONSTRAINT_TEXT,
  TARGET_STORAGE_CLASS_TEXT,
  TIER_TYPE,
  TIER_TYPE_DISPLAY,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';
import { BucketTieringUtils } from '../utils/rgw-bucket-tiering';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { FormatterService } from '~/app/shared/services/formatter.service';

type StorageClassSectionDetails = Partial<StorageClassDetails> & {
  zonegroup_name?: string;
  placement_target?: string;
  storage_class?: string;
  tier_type?: string;
  region?: string;
  endpoint?: string;
  acl_mappings?: ACL[];
  acls?: ACL[];
};

@Component({
  selector: 'cd-rgw-storage-class-resource-page',
  templateUrl: './rgw-storage-class-resource-page.component.html',
  styleUrls: ['./rgw-storage-class-resource-page.component.scss'],
  standalone: false
})
export class RgwStorageClassResourcePageComponent implements OnInit, OnDestroy {
  private detailsCache = new Map<string, StorageClassSectionDetails>();
  private sub = new Subscription();
  private readonly placementTargetText = $localize`Placement targets control which Pools are associated with a particular bucket.`;
  private readonly zoneText = $localize`A zone defines a logical group that consists of one or more Ceph Object Gateway instances.`;
  private readonly dataPoolText = $localize`The data pool contains the objects associated with this storage class.`;

  section = '';
  storageClassName = '';
  zonegroupName = '';
  placementTarget = '';
  details?: StorageClassDetails;
  aclLoading = false;
  overviewLoading = false;
  overviewFields: OverviewField[] = [];
  aclKeyValueData: [string, string][] = [];

  constructor(
    private route: ActivatedRoute,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwZoneService: RgwZoneService,
    private formatter: FormatterService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.zonegroupName = pm.get('zonegroup_name') ?? '';
        this.placementTarget = pm.get('placement_target') ?? '';
        this.storageClassName = pm.get('storage_class') ?? '';
        this.detailsCache.clear();
        this.loadStorageClassDetails();
      })
    );

    this.sub.add(
      this.route.data.subscribe((data) => {
        this.section = data['section'] ?? '';
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private loadStorageClassDetails(): void {
    if (!this.zonegroupName || !this.placementTarget || !this.storageClassName) {
      this.details = undefined;
      this.aclLoading = false;
      this.overviewLoading = false;
      this.overviewFields = this.buildOverviewFields();
      this.aclKeyValueData = this.buildAclKeyValueData();
      return;
    }

    const cacheKey = this.getCacheKey();
    const cachedDetails = this.detailsCache.get(cacheKey);
    if (cachedDetails) {
      this.aclLoading = false;
      this.overviewLoading = false;
      this.details = {
        ...cachedDetails,
        acl_mappings: cachedDetails.acl_mappings || cachedDetails.acls || []
      } as StorageClassDetails;
      this.overviewFields = this.buildOverviewFields(this.details);
      this.aclKeyValueData = this.buildAclKeyValueData(this.details.acl_mappings || []);

      if (this.details.tier_type?.toLowerCase() === TIER_TYPE.LOCAL) {
        this.loadLocalZoneDetails(this.details);
      }
      return;
    }

    this.aclLoading = true;
    this.overviewLoading = true;

    this.sub.add(
      this.rgwZonegroupService
        .getAllZonegroupsInfo()
        .pipe(finalize(() => (this.overviewLoading = false)))
        .subscribe(
          (data: ZoneGroupDetails) => {
            const tierConfig = BucketTieringUtils.filterAndMapTierTargets(data).map((tier) => ({
              ...tier,
              tier_type: BucketTieringUtils.mapTierTypeDisplay(tier.tier_type)
            })) as StorageClassSectionDetails[];

            const selected = tierConfig.find(
              (tier) =>
                tier.zonegroup_name === this.zonegroupName &&
                tier.placement_target === this.placementTarget &&
                tier.storage_class === this.storageClassName
            );

            this.aclLoading = false;
            if (selected) {
              this.detailsCache.set(cacheKey, selected);
            }

            this.details = selected
              ? ({
                  ...selected,
                  acl_mappings: selected.acl_mappings || selected.acls || []
                } as StorageClassDetails)
              : undefined;
            this.overviewFields = this.buildOverviewFields(this.details);
            this.aclKeyValueData = this.buildAclKeyValueData(this.details?.acl_mappings || []);

            if (this.details?.tier_type?.toLowerCase() === TIER_TYPE.LOCAL) {
              this.loadLocalZoneDetails(this.details);
            }
          },
          () => {
            this.aclLoading = false;
            this.details = undefined;
            this.overviewFields = this.buildOverviewFields();
            this.aclKeyValueData = this.buildAclKeyValueData();
          }
        )
    );
  }

  private loadLocalZoneDetails(details: StorageClassDetails): void {
    this.sub.add(
      this.rgwZoneService.getAllZonesInfo().subscribe(
        (data: AllZonesResponse) => {
          const localDetails = BucketTieringUtils.getZoneInfoHelper(data?.zones, details);
          this.details = {
            ...details,
            zone_name: localDetails.zone_name,
            data_pool: localDetails.data_pool
          };
          this.overviewFields = this.buildOverviewFields(this.details);
        },
        () => {
          this.overviewFields = this.buildOverviewFields(this.details);
        }
      )
    );
  }

  private getCacheKey(): string {
    return `${this.zonegroupName}/${this.placementTarget}/${this.storageClassName}`;
  }

  private buildOverviewFields(details?: StorageClassSectionDetails): OverviewField[] {
    const fields: OverviewField[] = [
      {
        label: $localize`Name`,
        value: details?.storage_class || this.storageClassName
      },
      {
        label: $localize`Type`,
        value: details?.tier_type
      },
      {
        label: $localize`Zonegroup`,
        value: details?.zonegroup_name
      },
      {
        label: $localize`Placement Target`,
        value: details?.placement_target,
        helperText: this.placementTargetText
      }
    ];

    if (this.isCloudTier(details)) {
      fields.push(
        {
          label: $localize`Target Path`,
          value: details?.target_path,
          helperText: TARGET_PATH_TEXT
        },
        {
          label: $localize`Target storage class`,
          value: details?.target_storage_class,
          helperText: TARGET_STORAGE_CLASS_TEXT,
          emptyText: '-'
        },
        {
          label: $localize`Access key`,
          value: details?.access_key,
          type: 'password',
          helperText: TARGET_ACCESS_KEY_TEXT
        },
        {
          label: $localize`Host Style`,
          value: details?.host_style,
          helperText: HOST_STYLE
        },
        {
          label: $localize`Head Object (Stub File)`,
          value: details?.retain_head_object ? $localize`Enabled` : $localize`Disabled`,
          helperText: RETAIN_HEAD_OBJECT_TEXT
        },
        {
          label: $localize`Allow Read Through`,
          value: details?.allow_read_through ? $localize`Enabled` : $localize`Disabled`,
          helperText: ALLOW_READ_THROUGH_TEXT
        }
      );

      if (details?.allow_read_through) {
        fields.push({
          label: $localize`Read through Restore Days`,
          value: this.toDays(details?.read_through_restore_days),
          helperText: READTHROUGH_RESTORE_DAYS_TEXT
        });
      }

      fields.push(
        {
          label: $localize`Restore Storage Class`,
          value: details?.restore_storage_class,
          helperText: RESTORE_STORAGE_CLASS_TEXT
        },
        {
          label: $localize`Multipart Minimum Part Size`,
          value: this.formatter.formatToBinary(details?.multipart_min_part_size, false, 1),
          helperText: MULTIPART_MIN_PART_TEXT
        },
        {
          label: $localize`Multipart Sync Threshold`,
          value: this.formatter.formatToBinary(details?.multipart_sync_threshold, false, 1),
          helperText: MULTIPART_SYNC_THRESHOLD_TEXT
        },
        {
          label: $localize`Target Region`,
          value: details?.region
        },
        {
          label: $localize`Location constraint`,
          value: details?.location_constraint,
          helperText: LOCATION_CONSTRAINT_TEXT
        },
        {
          label: $localize`Target Endpoint`,
          value: details?.endpoint
        }
      );
    }

    if (this.isGlacierTier(details)) {
      fields.push(
        {
          label: $localize`Glacier Restore Days`,
          value: this.toDays(details?.glacier_restore_days),
          helperText: GLACIER_RESTORE_DAY_TEXT
        },
        {
          label: $localize`Glacier Restore Tier Type`,
          value: details?.glacier_restore_tier_type,
          helperText: GLACIER_RESTORE_TIER_TYPE_TEXT
        }
      );
    }

    if (this.isLocalTier(details)) {
      fields.push(
        {
          label: $localize`Zone`,
          value: details?.zone_name,
          helperText: this.zoneText
        },
        {
          label: $localize`Data Pool`,
          value: details?.data_pool,
          helperText: this.dataPoolText
        }
      );
    }

    return fields;
  }

  private buildAclKeyValueData(acls: ACL[] = []): [string, string][] {
    const grouped = (acls || []).reduce((acc: Record<string, string[]>, item: ACL) => {
      const type = (item?.val?.type || '').toLowerCase();
      if (!type) {
        return acc;
      }
      const pair = `${item?.val?.source_id || '-'} : ${item?.val?.dest_id || '-'}`;
      acc[type] = acc[type] || [];
      acc[type].push(pair);
      return acc;
    }, {});

    return [
      ['Email', grouped['email']?.join(', ') || '-'],
      ['ID', grouped['id']?.join(', ') || '-'],
      ['URI', grouped['uri']?.join(', ') || '-']
    ];
  }

  private isCloudTier(details?: StorageClassSectionDetails): boolean {
    const tier = details?.tier_type?.toLowerCase();
    return (
      tier === TIER_TYPE_DISPLAY.CLOUD_TIER.toLowerCase() ||
      tier === TIER_TYPE_DISPLAY.GLACIER.toLowerCase()
    );
  }

  private isGlacierTier(details?: StorageClassSectionDetails): boolean {
    return details?.tier_type?.toLowerCase() === TIER_TYPE_DISPLAY.GLACIER.toLowerCase();
  }

  private isLocalTier(details?: StorageClassSectionDetails): boolean {
    return details?.tier_type?.toLowerCase() === TIER_TYPE_DISPLAY.LOCAL.toLowerCase();
  }

  private toDays(value?: number): string {
    return value == null ? '-' : `${value} ${value === 1 ? 'Day' : 'Days'}`;
  }
}
