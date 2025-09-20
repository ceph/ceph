import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
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
  GroupedACLs
} from '../models/rgw-storage-class.model';
@Component({
  selector: 'cd-rgw-storage-class-details',
  templateUrl: './rgw-storage-class-details.component.html',
  styleUrls: ['./rgw-storage-class-details.component.scss']
})
export class RgwStorageClassDetailsComponent implements OnChanges, OnInit {
  @Input()
  selection: StorageClassDetails;
  columns: CdTableColumn[] = [];
  storageDetails: StorageClassDetails;
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

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['selection']) {
      this.storageDetails = {
        zonegroup_name: this.selection?.zonegroup_name,
        placement_targets: this.selection?.placement_targets,
        access_key: this.selection?.access_key,
        secret: this.selection?.secret,
        target_path: this.selection?.target_path,
        tier_type: this.selection?.tier_type,
        multipart_min_part_size: this.selection?.multipart_min_part_size,
        multipart_sync_threshold: this.selection?.multipart_sync_threshold,
        host_style: this.selection?.host_style,
        retain_head_object: this.selection?.retain_head_object,
        allow_read_through: this.selection?.allow_read_through,
        glacier_restore_days: this.selection?.glacier_restore_days,
        glacier_restore_tier_type: this.selection?.glacier_restore_tier_type,
        restore_storage_class: this.selection?.restore_storage_class,
        read_through_restore_days: this.selection?.read_through_restore_days
      };
    }
  }

  ngOnInit() {
    this.groupedACLs = this.groupByType(this.selection.acl_mappings);
  }

  isTierMatch(...types: string[]): boolean {
    const tier_type = this.selection.tier_type?.toLowerCase();
    return types.some((type) => type.toLowerCase() === tier_type);
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
