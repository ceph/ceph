import { Component, Input, OnChanges } from '@angular/core';
import { rgwEncryptionConfigKeys } from '~/app/shared/models/rgw-encryption-config-keys';

@Component({
  selector: 'cd-rgw-config-details',
  templateUrl: './rgw-config-details.component.html',
  styleUrls: ['./rgw-config-details.component.scss']
})
export class RgwConfigDetailsComponent implements OnChanges {
  transformedData: {};
  @Input()
  selection: any;

  @Input()
  excludeProps: any[] = [];
  filteredEncryptionConfigValues: {};

  ngOnChanges(): void {
    if (this.selection) {
      this.filteredEncryptionConfigValues = Object.keys(this.selection)
        .filter((key) => !this.excludeProps.includes(key))
        .reduce((obj, key) => {
          obj[key] = this.selection[key];
          return obj;
        }, {});
      const transformedData = {};
      for (const key in this.filteredEncryptionConfigValues) {
        if (rgwEncryptionConfigKeys[key]) {
          transformedData[rgwEncryptionConfigKeys[key]] = this.filteredEncryptionConfigValues[key];
        } else {
          transformedData[key] = this.filteredEncryptionConfigValues[key];
        }
      }
      this.transformedData = transformedData;
    }
  }
}
