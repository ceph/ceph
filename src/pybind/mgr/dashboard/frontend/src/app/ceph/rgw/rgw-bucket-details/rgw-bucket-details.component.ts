import { Component, Input, OnChanges } from '@angular/core';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-rgw-bucket-details',
  templateUrl: './rgw-bucket-details.component.html',
  styleUrls: ['./rgw-bucket-details.component.scss']
})
export class RgwBucketDetailsComponent implements OnChanges {
  bucket: any;

  @Input()
  selection: CdTableSelection;

  constructor() {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.bucket = this.selection.first();
    }
  }
}
