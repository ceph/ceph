import { Component, Input, OnChanges } from '@angular/core';

const keyMapping = {
  auth: 'Authentication Method',
  encryption_type: 'Encryption Type',
  backend: 'Backend',
  prefix: 'Prefix',
  namespace: 'Namespace',
  secret_engine: 'Secret Engine',
  addr: 'Address',
  token_file: 'Token File',
  ssl_cacert: 'SSL CA Certificate',
  ssl_clientcert: 'SSL Client Certificate',
  ssl_clientkey: 'SSL Client Key',
  verify_ssl: 'Verify SSL'
};

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
  configData: any = [];

  ngOnChanges(): void {
    if (this.selection) {
      const transformedData = {};
      for (const key in this.selection) {
        if (keyMapping[key]) {
          transformedData[keyMapping[key]] = this.selection[key];
        } else {
          transformedData[key] = this.selection[key];
        }
      }
      this.transformedData = transformedData;
    }
  }
}
