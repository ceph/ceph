import { Component, OnDestroy, OnInit } from '@angular/core';
import { HttpParams } from '@angular/common/http';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { HostService } from '~/app/shared/api/host.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { HostOverviewDetails, STATUS_MAP, getStatus } from '~/app/shared/models/host.interface';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { FormatterService } from '~/app/shared/services/formatter.service';

@Component({
  selector: 'cd-host-resource-page',
  templateUrl: './host-resource-page.component.html',
  standalone: false
})
export class HostResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  hostname = '';
  section = '';
  permissions: Permissions;
  hostOverviewFields: OverviewField[] = [];

  constructor(
    private route: ActivatedRoute,
    private hostService: HostService,
    private authStorageService: AuthStorageService,
    private formatter: FormatterService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.hostname = pm.get('hostname') ?? '';
        this.loadOverview();
      })
    );
    this.section = this.route.snapshot.data['section'] ?? '';
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private loadOverview(): void {
    if (!this.hostname) {
      this.hostOverviewFields = [];
      return;
    }

    let params = new HttpParams();
    params = params.set('offset', '0');
    params = params.set('limit', '1');
    params = params.set('search', this.hostname);
    params = params.set('sort', '+hostname');

    this.sub.add(
      this.hostService.list(params, 'true').subscribe({
        next: (hosts: object[]) => {
          const hostList = (Array.isArray(hosts) ? hosts : []) as HostOverviewDetails[];
          const host = hostList.find((item) => item.hostname === this.hostname);
          this.hostOverviewFields = this.buildOverviewFields(host);
        },
        error: () => {
          this.hostOverviewFields = this.buildOverviewFields();
        }
      })
    );
  }

  private buildOverviewFields(host: Partial<HostOverviewDetails> = {}): OverviewField[] {
    const hostStatus = STATUS_MAP[getStatus(host)];
    const hostnameWithAddr = host?.addr ? `${this.hostname} (${host.addr})` : this.hostname;
    const totalMemory = this.formatter.formatToBinary(
      this.hostService.getTotalMemoryBytes(host),
      false,
      1
    );
    const rawCapacity = this.formatter.formatToBinary(
      this.hostService.getRawCapacityBytes(host),
      false,
      1
    );

    return [
      {
        label: $localize`Hostname`,
        value: hostnameWithAddr,
        type: 'text'
      },
      {
        label: $localize`Labels`,
        values: host?.labels ?? [],
        type: 'tags'
      },
      {
        label: $localize`Status`,
        value: hostStatus?.label,
        type: 'status',
        status: hostStatus?.icon
      },
      {
        label: $localize`Model`,
        value: host?.model,
        type: 'text'
      },
      {
        label: $localize`CPUs`,
        value: host?.cpu_count,
        type: 'text'
      },
      {
        label: $localize`Cores`,
        value: host?.cpu_cores,
        type: 'text'
      },
      {
        label: $localize`Total Memory`,
        value: totalMemory
      },
      {
        label: $localize`Raw Capacity`,
        value: rawCapacity
      },
      {
        label: $localize`HDDs`,
        value: host?.hdd_count,
        type: 'text'
      },
      {
        label: $localize`Flash`,
        value: host?.flash_count,
        type: 'text'
      },
      {
        label: $localize`NICs`,
        value: host?.nic_count,
        type: 'text'
      }
    ];
  }
}
