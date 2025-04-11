import { Component, Input, OnChanges } from '@angular/core';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { StorageClassDetails } from '../models/rgw-storage-class.model';

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

  ngOnChanges() {
    if (this.selection) {
      this.storageDetails = {
        access_key: this.selection.access_key,
        secret: this.selection.secret,
        target_path: this.selection.target_path,
        multipart_min_part_size: this.selection.multipart_min_part_size,
        multipart_sync_threshold: this.selection.multipart_sync_threshold,
        host_style: this.selection.host_style
      };
    }
  }
}
