import {
  Component,
  OnDestroy,
  OnInit,
  TemplateRef,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';

import _ from 'lodash';
import {
  KeyRow,
  ExtendedRgwUser,
  UserQuota,
  BucketQuota,
  Key,
  SwiftKey,
  RGW_MAX_BUCKETS_MAP
} from '~/app/ceph/rgw/models/rgw-user';
import { RgwUserS3KeyModalComponent } from '../rgw-user-s3-key-modal/rgw-user-s3-key-modal.component';
import { RgwUserSwiftKeyModalComponent } from '../rgw-user-swift-key-modal/rgw-user-swift-key-modal.component';
import { USER } from '~/app/shared/constants/app.constants';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
@Component({
  selector: 'cd-rgw-user-resource-page',
  templateUrl: './rgw-user-resource-page.component.html',
  styleUrls: ['./rgw-user-resource-page.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwUserResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  @ViewChild('viewKeyTpl', { static: true })
  declare viewKeyTpl: TemplateRef<any>;

  section = 'overview';
  user?: ExtendedRgwUser;
  selection?: ExtendedRgwUser;
  notFound = false;
  keys: KeyRow[] = [];
  keysColumns: CdTableColumn[] = [];
  userQuota: Record<string, string | number> = {};
  bucketQuota: Record<string, string | number> = {};
  overviewFields: OverviewField[] = [];

  constructor(
    private route: ActivatedRoute,
    private cdsModalService: ModalCdsService,
    private dimlessBinary: DimlessBinaryPipe
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';
    this.keysColumns = [
      {
        name: $localize`Username`,
        prop: 'username',
        flexGrow: 1
      },
      {
        name: $localize`Type`,
        prop: 'type',
        flexGrow: 1
      },
      {
        name: $localize`View`,
        prop: 'view',
        flexGrow: 1,
        cellTemplate: this.viewKeyTpl
      }
    ];

    this.sub.add(
      this.route.parent?.data.subscribe((data) => {
        this.applyUser(data?.user ?? null);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyUser(user: ExtendedRgwUser | null): void {
    this.notFound = !user;

    if (!user) {
      this.user = undefined;
      this.selection = undefined;
      this.overviewFields = [];
      this.userQuota = {};
      this.bucketQuota = {};
      this.keys = [];
      return;
    }

    this.user = user;
    this.selection = user;
    this.overviewFields = this.buildOverviewFields(this.user, this.selection);
    this.userQuota = this.createDisplayValues(this.user?.user_quota);
    this.bucketQuota = this.createDisplayValues(this.user?.bucket_quota);
    this.processKeys();
  }

  private buildOverviewFields(user: ExtendedRgwUser, selection: ExtendedRgwUser): OverviewField[] {
    const fields: OverviewField[] = [
      {
        label: $localize`Username`,
        value: user?.uid
      },
      {
        label: $localize`Tenant`,
        value: user?.tenant
      },
      {
        label: $localize`Account name`,
        value: selection?.account?.name
      },
      {
        label: $localize`Full name`,
        value: user?.display_name
      },
      {
        label: $localize`Email`,
        value: user?.email
      },
      {
        label: $localize`Suspended`,
        value: user?.suspended ? $localize`Yes` : $localize`No`
      },
      {
        label: $localize`System user`,
        value: user?.system ? $localize`Yes` : $localize`No`
      },
      {
        label: $localize`Max buckets`,
        value: RGW_MAX_BUCKETS_MAP[`${user.max_buckets}`] || `${user.max_buckets}`
      },
      {
        label: $localize`Capacity limit`,
        value: this.getQuotaUsageText('size'),
        emptyText: $localize`No Limit`
      },
      {
        label: $localize`Object limit`,
        value: this.getQuotaUsageText('object'),
        emptyText: $localize`No Limit`
      },
      {
        label: $localize`Managed policies`,
        value: user?.managed_user_policies
          ?.map((arn) => arn?.trim()?.split('/').pop())
          .filter(Boolean)
          .join(', ')
      },
      {
        label: $localize`Subusers`,
        value: user?.subusers?.map((subuser) => `${subuser.id} (${subuser.permissions})`).join(', ')
      },
      {
        label: $localize`Capabilities`,
        value: user?.caps?.map((cap) => `${cap.type} (${cap.perm})`).join(', ')
      },
      {
        label: $localize`MFAs (Id)`,
        value: user?.mfa_ids?.join(', ')
      }
    ];

    if (selection?.account?.id) {
      fields.push(
        ...[
          {
            label: $localize`Account ID`,
            value: selection?.account?.id
          },
          {
            label: $localize`Name`,
            value: selection?.account?.name
          },
          {
            label: $localize`Tenant`,
            value: selection?.account?.tenant
          },
          {
            label: $localize`User type`,
            value: user?.type === 'root' ? $localize`Account root user` : $localize`rgw user`
          }
        ]
      );
    }

    return fields;
  }

  private getQuotaUsageText(kind: 'size' | 'object'): string | null {
    const quota = this.user?.user_quota;
    const stats = this.user?.stats;

    if (!quota?.enabled) return null;

    if (kind === 'size' && quota.max_size > 0) {
      const used = Number(stats?.size_actual ?? 0);
      return `${((used / quota.max_size) * 100).toFixed(1)}%`;
    }

    if (kind === 'object' && quota.max_objects > 0) {
      const used = Number(stats?.num_objects ?? 0);
      return `${((used / quota.max_objects) * 100).toFixed(1)}%`;
    }

    return null;
  }

  private createDisplayValues(quota?: UserQuota | BucketQuota): Record<string, string | number> {
    if (!quota) {
      return {};
    }

    return {
      [$localize`Enabled`]: quota.enabled ? $localize`Yes` : $localize`No`,
      [$localize`Maximum size`]: quota.enabled
        ? quota.max_size <= -1
          ? $localize`Unlimited`
          : this.dimlessBinary.transform(quota.max_size)
        : '-',
      [$localize`Maximum objects`]: quota.enabled
        ? quota.max_objects <= -1
          ? $localize`Unlimited`
          : quota.max_objects
        : '-'
    };
  }

  private processKeys(): void {
    this.keys = [];
    if (this.user?.keys) {
      this.user.keys.forEach((key: Key) => {
        this.keys.push({
          id: this.keys.length + 1,
          type: 'S3',
          username: key.user,
          ref: key
        });
      });
    }

    if (this.user?.swift_keys) {
      this.user.swift_keys.forEach((key: SwiftKey) => {
        this.keys.push({
          id: this.keys.length + 1,
          type: 'Swift',
          username: key.user,
          ref: key
        });
      });
    }

    this.keys = _.sortBy(this.keys, USER);
  }

  showKeyModal(key: KeyRow): void {
    if (!key) {
      return;
    }

    const modalRef = this.cdsModalService.show(
      key.type === 'S3' ? RgwUserS3KeyModalComponent : RgwUserSwiftKeyModalComponent
    );

    switch (key.type) {
      case 'S3':
        const s3Ref = key.ref as Key;
        modalRef.setViewing();
        modalRef.setValues(s3Ref.user, s3Ref.access_key, s3Ref.secret_key);
        break;
      case 'Swift':
        const swiftRef = key.ref as SwiftKey;
        modalRef.setValues(swiftRef.user, swiftRef.secret_key);
        break;
    }
  }
}
