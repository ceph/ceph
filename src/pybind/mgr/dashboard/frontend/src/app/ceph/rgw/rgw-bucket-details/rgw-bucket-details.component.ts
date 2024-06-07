import { Component, Input, OnChanges } from '@angular/core';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';

import * as xml2js from 'xml2js';

@Component({
  selector: 'cd-rgw-bucket-details',
  templateUrl: './rgw-bucket-details.component.html',
  styleUrls: ['./rgw-bucket-details.component.scss']
})
export class RgwBucketDetailsComponent implements OnChanges {
  @Input()
  selection: any;

  aclPermissions: Record<string, string[]> = {};
  replicationStatus = $localize`Disabled`;

  constructor(private rgwBucketService: RgwBucketService) {}

  ngOnChanges() {
    if (this.selection) {
      this.rgwBucketService.get(this.selection.bid).subscribe((bucket: object) => {
        bucket['lock_retention_period_days'] = this.rgwBucketService.getLockDays(bucket);
        this.selection = bucket;
        this.aclPermissions = this.parseXmlAcl(this.selection.acl, this.selection.owner);
        if (this.selection.replication?.['Rule']?.['Status']) {
          this.replicationStatus = this.selection.replication?.['Rule']?.['Status'];
        }
      });
    }
  }

  parseXmlAcl(xml: any, bucketOwner: string): Record<string, string[]> {
    const parser = new xml2js.Parser({ explicitArray: false, trim: true });
    let data: Record<string, string[]> = {
      Owner: ['-'],
      AllUsers: ['-'],
      AuthenticatedUsers: ['-']
    };
    parser.parseString(xml, (err, result) => {
      if (err) return null;

      const xmlGrantees: any = result['AccessControlPolicy']['AccessControlList']['Grant'];
      if (Array.isArray(xmlGrantees)) {
        for (let i = 0; i < xmlGrantees.length; i++) {
          const grantee = xmlGrantees[i];
          if (grantee?.Grantee?.URI) {
            const granteeGroup = grantee.Grantee.URI.split('/').pop();
            if (data[granteeGroup].includes('-')) {
              data[granteeGroup] = [grantee?.Permission];
            } else {
              data[granteeGroup].push(grantee?.Permission);
            }
          }
          if (grantee?.Grantee?.ID && bucketOwner === grantee?.Grantee?.ID) {
            data['Owner'] = grantee?.Permission;
          }
        }
      } else {
        if (xmlGrantees?.Grantee?.ID && bucketOwner === xmlGrantees?.Grantee?.ID) {
          data['Owner'] = xmlGrantees?.Permission;
        }
      }
    });
    return data;
  }
}
