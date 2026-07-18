import { ChangeDetectorRef, Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { Subscription } from 'rxjs';
import * as xml2js from 'xml2js';
import { ContentSwitcherOption } from 'carbon-components-angular';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { Bucket } from '../models/rgw-bucket';
import { RgwBucketReplication } from '../models/rgw-bucket-replication';
import { RgwRateLimitConfig } from '../models/rgw-rate-limit';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';

@Component({
  selector: 'cd-rgw-bucket-resource-page',
  templateUrl: './rgw-bucket-resource-page.component.html',
  styleUrls: ['./rgw-bucket-resource-page.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwBucketResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  bid = '';
  owner = '';
  section = '';
  selection: Bucket | undefined;

  lifecycleProgress: { bucket: string; status: string; started: string } = {
    bucket: '',
    status: 'UNINITIAL',
    started: ''
  };
  lifecycleContentSwitcherSelectedIndex = 0;
  lifecycleContentSwitcherSelectionMode = 'single' as const;
  lifecycleProgressMap = new Map<string, { description: string; color: string }>([
    ['UNINITIAL', { description: $localize`The process has not run yet`, color: 'cool-gray' }],
    ['PROCESSING', { description: $localize`The process is currently running`, color: 'cyan' }],
    ['COMPLETE', { description: $localize`The process has completed`, color: 'green' }]
  ]);
  lifecycleFormat: 'json' | 'xml' = 'json';

  aclPermissions: Record<string, string[]> = {};
  replicationStatus = $localize`Disabled`;
  hasSyncPolicyOnly = false;
  replicationData: RgwBucketReplication = {
    sync_policy_active: false,
    replication_rules_configured: false,
    policy: {}
  };
  bucketRateLimit: RgwRateLimitConfig = {} as RgwRateLimitConfig;

  /* Data arrays for template binding */
  overviewData: Array<{ key: string; value: any }> = [];
  detailsData: Array<{ key: string; value: any }> = [];
  lockingData: Array<{ key: string; value: any }> = [];
  quotaData: Array<{ key: string; value: any }> = [];
  aclData: Array<{ key: string; value: any }> = [];

  overviewFields: OverviewField[] = [];
  configurationSummaryFields: OverviewField[] = [];

  constructor(
    private route: ActivatedRoute,
    private rgwBucketService: RgwBucketService,
    private cd: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? '';

    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.bid = pm.get('bid') ?? '';
        this.owner = pm.get('owner') ?? '';
        this.updateBucketDetails();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  updateBucketDetails() {
    if (!this.bid) {
      this.selection = undefined;
      this.overviewFields = [];
      this.configurationSummaryFields = [];
      return;
    }

    this.sub.add(
      this.rgwBucketService.get(this.bid).subscribe((bucket: Bucket) => {
        const bucketDetails = {
          ...bucket,
          lock_retention_period_days: this.rgwBucketService.getLockDays(bucket)
        };
        this.selection = bucketDetails;
        this.extractDetailsFromResponse();
      })
    );
  }

  extractDetailsFromResponse() {
    if (!this.selection) return;

    this.aclPermissions = this.parseXmlAcl(this.selection.acl, this.selection.owner);
    this.extractReplicationDetails();
    this.fetchRateLimit();
    this.extractLifecycleDetails();
    this.overviewFields = this.buildOverviewFields();
    this.configurationSummaryFields = this.buildConfigurationSummaryFields();
  }

  private toFields(rows: Array<{ key: string; value: any }>): OverviewField[] {
    return rows.map((row) => ({
      label: row.key,
      value: Array.isArray(row.value) ? row.value.join(', ') : row.value
    }));
  }

  private extractReplicationDetails() {
    const repl = this.selection.replication;
    this.replicationData = {
      sync_policy_active: repl?.sync_policy_active === true,
      replication_rules_configured: repl?.replication_rules_configured === true,
      policy: repl?.policy || {}
    };

    this.hasSyncPolicyOnly =
      this.replicationData.sync_policy_active && !this.replicationData.replication_rules_configured;

    if (this.replicationData.sync_policy_active) {
      this.replicationStatus = $localize`Enabled`;
    } else if (
      this.replicationData.replication_rules_configured &&
      this.replicationData.policy['Rule']?.['Status']
    ) {
      this.replicationStatus = this.replicationData.policy['Rule']['Status'];
    } else {
      this.replicationStatus = $localize`Disabled`;
    }
  }

  private fetchRateLimit() {
    this.sub.add(
      this.rgwBucketService.getBucketRateLimit(this.selection.bid).subscribe((resp: any) => {
        if (resp?.bucket_ratelimit !== undefined) {
          this.bucketRateLimit = resp.bucket_ratelimit;
        }
      })
    );
  }

  parseXmlAcl(xml: any, bucketOwner: string): Record<string, string[]> {
    const parser = new xml2js.Parser({ explicitArray: false, trim: true });
    const data: Record<string, string[]> = {
      Owner: ['-'],
      AllUsers: ['-'],
      AuthenticatedUsers: ['-']
    };

    parser.parseString(xml, (err, result) => {
      if (err) return;

      const xmlGrantees = result?.['AccessControlPolicy']?.['AccessControlList']?.['Grant'];
      if (!xmlGrantees) return;

      if (Array.isArray(xmlGrantees)) {
        xmlGrantees.forEach((grantee) => this.processGrantee(grantee, bucketOwner, data));
      } else {
        this.processGrantee(xmlGrantees, bucketOwner, data);
      }
    });

    return data;
  }

  private processGrantee(grantee: any, bucketOwner: string, data: Record<string, string[]>) {
    if (grantee?.Grantee?.URI) {
      const granteeGroup = grantee.Grantee.URI.split('/').pop();
      if (data[granteeGroup!]) {
        if (data[granteeGroup!].includes('-')) {
          data[granteeGroup!] = [grantee.Permission];
        } else {
          data[granteeGroup!].push(grantee.Permission);
        }
      }
    }
    if (grantee?.Grantee?.ID && bucketOwner === grantee.Grantee.ID) {
      data['Owner'] = grantee.Permission;
    }
  }

  private buildOverviewFields(): OverviewField[] {
    const bucketQuota = this.selection?.bucket_quota;

    this.overviewData = [
      { key: $localize`Tenant`, value: this.selection?.tenant },
      { key: $localize`Size (bytes)`, value: this.selection?.bucket_size }
    ];

    this.detailsData = [
      { key: $localize`Versioning`, value: this.selection?.versioning },
      { key: $localize`Encryption`, value: this.selection?.encryption },
      { key: $localize`Replication`, value: this.replicationStatus },
      { key: $localize`MFA Delete`, value: this.selection?.mfa_delete },
      { key: $localize`Index type`, value: this.selection?.index_type },
      { key: $localize`Placement rule`, value: this.selection?.placement_rule }
    ];

    this.lockingData = this.selection?.lock_enabled
      ? [
          {
            key: $localize`Locking mode`,
            value:
              this.selection.lock_mode.charAt(0).toUpperCase() +
              this.selection.lock_mode.slice(1).toLowerCase()
          },
          { key: $localize`Days`, value: this.selection?.lock_retention_period_days }
        ]
      : [];

    const formatQuota = (val: number) => (val <= -1 ? $localize`Unlimited` : val);

    this.quotaData = [
      { key: $localize`Bucket Quota Enabled`, value: bucketQuota?.enabled || false },
      {
        key: $localize`Maximum size`,
        value: bucketQuota?.enabled ? formatQuota(bucketQuota.max_size) : '-'
      },
      {
        key: $localize`Maximum objects`,
        value: bucketQuota?.enabled ? formatQuota(bucketQuota.max_objects) : '-'
      }
    ];

    this.aclData = [
      { key: $localize`Bucket Owner`, value: this.aclPermissions.Owner || '-' },
      { key: $localize`Everyone`, value: this.aclPermissions.AllUsers || '-' },
      {
        key: $localize`Authenticated users group`,
        value: this.aclPermissions.AuthenticatedUsers || '-'
      }
    ];

    return [
      { label: $localize`Bucket Name`, value: this.selection?.bid },
      { label: $localize`Owner`, value: this.selection?.owner },
      {
        label: $localize`Used Capacity`,
        value: this.selection?.bucket_size,
        emptyText: $localize`0 B`
      },
      {
        label: $localize`Capacity Limit %`,
        value: this.selection?.size_usage,
        emptyText: $localize`No Limit`
      },
      { label: $localize`Objects`, value: this.selection?.num_objects, emptyText: $localize`0` },
      {
        label: $localize`Object Limit %`,
        value: this.selection?.object_usage,
        emptyText: $localize`No Limit`
      },
      { label: $localize`Number of Shards`, value: this.selection?.num_shards },
      { label: $localize`Last modification time`, value: this.selection?.mtime },
      ...this.toFields(this.overviewData),
      ...this.toFields(this.quotaData)
    ];
  }

  private buildConfigurationSummaryFields(): OverviewField[] {
    return [...this.toFields(this.detailsData), ...this.toFields(this.lockingData)];
  }

  extractLifecycleDetails() {
    if (this.lifecycleFormat === 'json' && !this.selection.lifecycle) {
      this.selection.lifecycle = {};
    }
    if (this.selection.lifecycle_progress?.length > 0) {
      this.selection.lifecycle_progress.forEach(
        (progress: { bucket: string; status: string; started: string }) => {
          if (progress.bucket.includes(this.selection.bucket)) {
            this.lifecycleProgress = progress;
          }
        }
      );
    }
  }

  updateLifecycleFormatTo(format: 'json' | 'xml'): void {
    this.lifecycleFormat = format;
    this.lifecycleContentSwitcherSelectedIndex = format === 'json' ? 0 : 1;
    this.cd.detectChanges();
  }

  updateLifecycleFormatFromSwitcher(option: ContentSwitcherOption): void {
    this.updateLifecycleFormatTo(option.name as 'json' | 'xml');
  }
}
