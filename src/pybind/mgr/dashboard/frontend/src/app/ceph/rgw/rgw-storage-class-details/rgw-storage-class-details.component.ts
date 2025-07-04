import { Component, Input, OnChanges } from '@angular/core';
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
  TIER_TYPE_DISPLAY
} from '../models/rgw-storage-class.model';
@Component({
  selector: 'cd-rgw-storage-class-details',
  templateUrl: './rgw-storage-class-details.component.html',
  styleUrls: ['./rgw-storage-class-details.component.scss']
})
export class RgwStorageClassDetailsComponent implements OnChanges {
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

  ngOnChanges() {
    if (this.selection) {
      this.storageDetails = {
        zonegroup_name: this.selection.zonegroup_name,
        placement_targets: this.selection.placement_targets,
        access_key: this.selection.access_key,
        secret: this.selection.secret,
        target_path: this.selection.target_path,
        multipart_min_part_size: this.selection.multipart_min_part_size,
        multipart_sync_threshold: this.selection.multipart_sync_threshold,
        host_style: this.selection.host_style,
        retain_head_object: this.selection.retain_head_object,
        allow_read_through: this.selection.allow_read_through
      };
    }
  }
}
