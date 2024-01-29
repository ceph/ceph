import { Component, Input, OnChanges } from '@angular/core';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';

@Component({
  selector: 'cd-rgw-bucket-details',
  templateUrl: './rgw-bucket-details.component.html',
  styleUrls: ['./rgw-bucket-details.component.scss']
})
export class RgwBucketDetailsComponent implements OnChanges {
  @Input()
  selection: any;

  constructor(private rgwBucketService: RgwBucketService) {}

  ngOnChanges() {
    if (this.selection) {
      this.rgwBucketService.get(this.selection.bid).subscribe((bucket: object) => {
        bucket['lock_retention_period_days'] = this.rgwBucketService.getLockDays(bucket);
        this.selection = bucket;
        this.selection.bucket_policy = JSON.parse(this.selection.bucket_policy) || {};
      });
    }
  }
}
