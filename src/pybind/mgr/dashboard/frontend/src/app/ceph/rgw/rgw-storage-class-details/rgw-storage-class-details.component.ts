import { Component, inject, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import {
  ALLOW_READ_THROUGH_TEXT,
  HOST_STYLE,
  MULTIPART_MIN_PART_TEXT,
  MULTIPART_SYNC_THRESHOLD_TEXT,
  RETAIN_HEAD_OBJECT_TEXT,
  StorageClassDetails,
  TARGET_ACCESS_KEY_TEXT,
  TARGET_PATH_TEXT,
  TARGET_SECRET_KEY_TEXT,
  TIER_TYPE_DISPLAY,
  TIER_TYPE,
  GLACIER_RESTORE_DAY_TEXT,
  GLACIER_RESTORE_TIER_TYPE_TEXT,
  RESTORE_DAYS_TEXT,
  READTHROUGH_RESTORE_DAYS_TEXT,
  RESTORE_STORAGE_CLASS_TEXT,
  ZONEGROUP_TEXT,
  ACL,
  GroupedACLs,
  AllZonesResponse
} from '../models/rgw-storage-class.model';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { BucketTieringUtils } from '../utils/rgw-bucket-tiering';
@Component({
  selector: 'cd-rgw-storage-class-details',
  templateUrl: './rgw-storage-class-details.component.html',
  styleUrls: ['./rgw-storage-class-details.component.scss']
})
export class RgwStorageClassDetailsComponent implements OnChanges, OnInit {
  @Input()
  selection: StorageClassDetails;
  columns: CdTableColumn[] = [];
  allowReadThroughText = ALLOW_READ_THROUGH_TEXT;
  retainHeadObjectText = RETAIN_HEAD_OBJECT_TEXT;
  multipartMinPartText = MULTIPART_MIN_PART_TEXT;
  multipartSyncThreholdText = MULTIPART_SYNC_THRESHOLD_TEXT;
  targetAccessKeyText = TARGET_ACCESS_KEY_TEXT;
  targetSecretKeyText = TARGET_SECRET_KEY_TEXT;
  targetPathText = TARGET_PATH_TEXT;
  hostStyleText = HOST_STYLE;
  TIER_TYPE_DISPLAY = TIER_TYPE_DISPLAY;
  TIER_TYPE = TIER_TYPE;
  glacierRestoreDayText = GLACIER_RESTORE_DAY_TEXT;
  glacierRestoreTiertypeText = GLACIER_RESTORE_TIER_TYPE_TEXT;
  restoreDaysText = RESTORE_DAYS_TEXT;
  readthroughrestoreDaysText = READTHROUGH_RESTORE_DAYS_TEXT;
  restoreStorageClassText = RESTORE_STORAGE_CLASS_TEXT;
  zoneGroupText = ZONEGROUP_TEXT;
  groupedACLs: GroupedACLs = {};
  localStorageClassDetails = { zone_name: '', data_pool: '' };
  loading = false;

  private rgwZoneService = inject(RgwZoneService);

  ngOnChanges(changes: SimpleChanges): void {
    if (
      changes['selection'] &&
      changes['selection'].currentValue?.tier_type?.toLowerCase() === TIER_TYPE.LOCAL &&
      changes['selection'].firstChange
    ) {
      // The idea here is to not call the API if we already have the zone_name and data_pool
      // When the details view is expanded and table refreshes data then this API should not be called again
      const { zone_name, data_pool } = this.localStorageClassDetails;
      if (!zone_name || !data_pool) {
        this.getZoneInfo();
      }
    }
  }

  ngOnInit() {
    this.groupedACLs = this.groupByType(this.selection?.acl_mappings);
  }

  isTierMatch(...types: string[]): boolean {
    const tier_type = this.selection.tier_type?.toLowerCase();
    return types.some((type) => type.toLowerCase() === tier_type);
  }

  getZoneInfo() {
    this.loading = true;
    this.rgwZoneService.getAllZonesInfo().subscribe({
      next: (data: AllZonesResponse) => {
        this.localStorageClassDetails = BucketTieringUtils.getZoneInfoHelper(
          data.zones,
          this.selection
        );
        this.loading = false;
      },
      error: () => {
        this.loading = false;
      }
    });
  }

  groupByType(acls: ACL[]): GroupedACLs {
    return acls?.reduce((groupAcls: GroupedACLs, item: ACL) => {
      const type = item.val?.type?.toUpperCase();
      groupAcls[type] = groupAcls[type] ?? [];
      groupAcls[type].push({
        source_id: item.val?.source_id,
        dest_id: item.val?.dest_id
      });
      return groupAcls;
    }, {});
  }
}
