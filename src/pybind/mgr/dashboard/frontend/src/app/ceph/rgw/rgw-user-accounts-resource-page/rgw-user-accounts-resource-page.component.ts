import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { Account } from '../models/rgw-user-accounts';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-rgw-user-accounts-resource-page',
  templateUrl: './rgw-user-accounts-resource-page.component.html',
  styleUrls: ['./rgw-user-accounts-resource-page.component.scss'],
  standalone: false
})
export class RgwUserAccountsResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = '';
  selection?: Account;
  notFound = false;
  overviewField: OverviewField[] = [];
  quota: Record<string, string | number> = {};
  bucket_quota: Record<string, string | number> = {};

  constructor(
    private route: ActivatedRoute,
    private dimlessBinary: DimlessBinaryPipe
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';

    this.sub.add(
      this.route.parent?.data.subscribe((data) => {
        const account = (data?.account ?? null) as Account | null;
        this.applyAccount(account);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyAccount(account: Account | null): void {
    this.notFound = !account;
    if (!account) {
      this.selection = undefined;
      this.overviewField = [];
      this.quota = {};
      this.bucket_quota = {};
      return;
    }

    this.selection = account;
    this.overviewField = this.buildOverviewFields(account);
    this.quota = this.createDisplayValues('quota');
    this.bucket_quota = this.createDisplayValues('bucket_quota');
  }

  private buildOverviewFields(account: Account): OverviewField[] {
    return [
      {
        label: $localize`Name`,
        value: account.name
      },
      {
        label: $localize`Tenant`,
        value: account.tenant
      },
      {
        label: $localize`Account id`,
        value: account.id
      },
      {
        label: $localize`Email address`,
        value: account.email
      },
      {
        label: $localize`Max users`,
        value: account.max_users
      },
      {
        label: $localize`Max roles`,
        value: account.max_roles
      },
      {
        label: $localize`Max groups`,
        value: account.max_groups
      },
      {
        label: $localize`Max. buckets`,
        value: account.max_buckets
      },
      {
        label: $localize`Max access keys`,
        value: account.max_access_keys
      }
    ];
  }

  private createDisplayValues(
    quotaType: 'quota' | 'bucket_quota'
  ): Record<string, string | number> {
    if (!this.selection?.[quotaType]) {
      return {};
    }

    const quota = this.selection[quotaType];
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
}
