import { Component, OnInit } from '@angular/core';

import { PrometheusService } from '../../../shared/api/prometheus.service';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '../../../shared/services/feature-toggles.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { I18n } from '@ngx-translate/i18n-polyfill';

interface MenuEntry {
  name: string
  link?: string
  perm?: boolean
  enable?: string
  children?: MenuEntries
};

type MenuEntries = MenuEntry[];

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  permissions: Permissions;
  summaryData: any;

  isCollapsed = true;
  prometheusConfigured = false;
  enabledFeature$: FeatureTogglesMap$;

  menuEntries:MenuEntries;

  constructor(
    private authStorageService: AuthStorageService,
    private prometheusService: PrometheusService,
    private summaryService: SummaryService,
    private featureToggles: FeatureTogglesService,
    private i18n: I18n
  ) {
    this.permissions = this.authStorageService.getPermissions();
    let permissions = this.permissions;
    this.enabledFeature$ = this.featureToggles.get();
    //this.enabledFeature$ = this.featureToggles.get();
    this.menuEntries = [
      {name: this.i18n('Dashboard'), link: '/dashboard',
        perm: permissions.hosts.read || permissions.monitor.read || permissions.osd.read || permissions.configOpt.read},

      {name: this.i18n('Cluster'), children: [
        {name: this.i18n('Hosts'), link: '/hosts', perm: permissions.hosts.read},
        {name: this.i18n('Monitors'), link: '/monitor', perm: permissions.monitor.read},
        {name: this.i18n('OSDs'), link: '/osd', perm: permissions.osd.read},
        {name: this.i18n('Configuration'), link: '/configuration', perm: permissions.configOpt.read},
        {name: this.i18n('CRUSH map'), link: '/crush-map', perm: permissions.hosts.read && permissions.osd.read},
        {name: this.i18n('Manager modules'), link: '/mgr-modules', perm: permissions.configOpt.read},
        {name: this.i18n('Logs'), link: '/logs', perm: permissions.log.read},
        {name: this.i18n('Alerts'), link: '/alerts', perm: this.prometheusConfigured && permissions.prometheus.read},
      ]},
      {name: this.i18n('Pools'), link: '/pool', perm: permissions.pool.read},

      {name: this.i18n('Block'), link: '/block/rbd', perm: (permissions.rbdImage.read || permissions.rbdMirroring.read || permissions.iscsi.read),
        enable: 'rbd'},
    ];
  }

  ngOnInit() {
    this.summaryService.subscribe((data: any) => {
      if (!data) {
        return;
      }
      this.summaryData = data;
    });
    this.prometheusService.ifAlertmanagerConfigured(() => (this.prometheusConfigured = true));
  }

  blockHealthColor() {
    if (this.summaryData && this.summaryData.rbd_mirroring) {
      if (this.summaryData.rbd_mirroring.errors > 0) {
        return { color: '#d9534f' };
      } else if (this.summaryData.rbd_mirroring.warnings > 0) {
        return { color: '#f0ad4e' };
      }
    }
  }
}
