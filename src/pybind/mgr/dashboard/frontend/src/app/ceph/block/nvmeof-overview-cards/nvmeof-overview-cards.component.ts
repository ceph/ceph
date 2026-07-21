import { Component, Input } from '@angular/core';
import { Observable } from 'rxjs';
import { NvmeofThroughput } from '~/app/shared/api/performance-card.service';

type OverviewResourceStats = {
  gatewayGroups: number;
  subsystems: number;
  namespaces: number;
  hosts: number;
  activeConnections: number;
};

type OverviewAlerts = {
  critical: number;
  warning: number;
  total: number;
  byCategory: Record<string, number>;
};

type OverviewCardId = 'resources' | 'alerts' | 'throughput';
type OverviewTabId = 'gateways' | 'subsystems' | 'namespaces';

type OverviewCardConfig = {
  id: OverviewCardId;
  title: string;
};

type ResourceRowConfig = {
  key: keyof Pick<OverviewResourceStats, 'gatewayGroups' | 'subsystems' | 'namespaces' | 'hosts'>;
  label: string;
  tab?: OverviewTabId;
};

type ThroughputRowConfig = {
  key: keyof Pick<NvmeofThroughput, 'reads' | 'writes'>;
  label: string;
};

@Component({
  selector: 'cd-nvmeof-overview-cards',
  templateUrl: './nvmeof-overview-cards.component.html',
  styleUrls: ['./nvmeof-overview-cards.component.scss'],
  standalone: false
})
export class NvmeofOverviewCardsComponent {
  @Input({ required: true }) stats: OverviewResourceStats;
  @Input({ required: true }) alerts$: Observable<OverviewAlerts>;
  @Input({ required: true }) throughput$: Observable<NvmeofThroughput>;
  @Input({ required: true }) alertQueryParams: (
    severity: 'all' | 'critical' | 'warning',
    category?: string
  ) => Record<string, string>;

  @Input() baseRoute = '/block/nvmeof';
  @Input() alertsRoute: string[] = ['/monitoring/active-alerts'];
  @Input() detailedInfoTab: OverviewTabId = 'gateways';
  @Input() alertsScopeLabel = $localize`All resources`;

  @Input() overviewCards: OverviewCardConfig[] = [
    { id: 'resources', title: $localize`Resources status` },
    { id: 'alerts', title: $localize`Alert notifications` },
    { id: 'throughput', title: $localize`Throughput` }
  ];

  @Input() resourceRows: ResourceRowConfig[] = [
    { key: 'gatewayGroups', label: $localize`Gateway groups`, tab: 'gateways' },
    { key: 'subsystems', label: $localize`Subsystems`, tab: 'subsystems' },
    { key: 'namespaces', label: $localize`Namespaces`, tab: 'namespaces' },
    { key: 'hosts', label: $localize`Hosts` }
  ];

  @Input() throughputRows: ThroughputRowConfig[] = [
    { key: 'reads', label: $localize`Reads` },
    { key: 'writes', label: $localize`Writes` }
  ];
}
