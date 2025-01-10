import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-rgw-user-accounts-details',
  templateUrl: './rgw-user-accounts-details.component.html',
  styleUrls: ['./rgw-user-accounts-details.component.scss']
})
export class RgwUserAccountsDetailsComponent implements OnChanges {
  @Input()
  selection: any;
  quota = {};
  bucket_quota = {};

  constructor(private dimlessBinary: DimlessBinaryPipe) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.selection && changes.selection.currentValue) {
      this.quota = this.createDisplayValues('quota');
      this.bucket_quota = this.createDisplayValues('bucket_quota');
    }
  }

  createDisplayValues(quota_type: string) {
    return {
      Enabled: this.selection[quota_type].enabled ? 'Yes' : 'No',
      'Maximum size': this.selection[quota_type].enabled
        ? this.selection[quota_type].max_size <= -1
          ? 'Unlimited'
          : this.dimlessBinary.transform(this.selection[quota_type].max_size)
        : '-',
      'Maximum objects': this.selection[quota_type].enabled
        ? this.selection[quota_type].max_objects <= -1
          ? 'Unlimited'
          : this.selection[quota_type].max_objects
        : '-'
    };
  }
}
