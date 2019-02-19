import { Component, Input, OnChanges, OnInit } from '@angular/core';
import { DomSanitizer, SafeUrl } from '@angular/platform-browser';

import { GrafanaTimesConstants } from '../../../shared/constants/grafanaTimes.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { SummaryService } from '../../../shared/services/summary.service';
import { SettingRegistryService } from '../../api/setting-registry.service';
import { SettingsService } from '../../api/settings.service';
@Component({
  selector: 'cd-grafana',
  templateUrl: './grafana.component.html',
  styleUrls: ['./grafana.component.scss']
})
export class GrafanaComponent implements OnInit, OnChanges {
  grafanaSrc: SafeUrl;
  url: string;
  protocol: string;
  host: string;
  port: number;
  baseUrl: any;
  panelStyle: any;
  grafanaExist = false;
  mode = '&kiosk';
  styles = {};
  dashboardExist = true;
  time: string;
  grafanaTimes: any;
  icons = Icons;
  readonly DEFAULT_TIME: string = 'from=now-1h&to=now';
  loading: boolean;
  uiSettings = {};

  @Input()
  grafanaPath: string;
  @Input()
  grafanaStyle: string;
  @Input()
  uid: string;

  docsUrl: string;
  refresh: any;

  constructor(
    private summaryService: SummaryService,
    private sanitizer: DomSanitizer,
    private settingsService: SettingsService,
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private settingRegistryService: SettingRegistryService
  ) {
    this.grafanaTimes = GrafanaTimesConstants.grafanaTimeData;
  }

  ngOnInit() {
    this.styles = {
      one: 'grafana_one',
      two: 'grafana_two',
      three: 'grafana_three'
    };

    this.settingsService
      .validateGrafanaDashboardUrl(this.uid)
      .subscribe((data: any) => (this.dashboardExist = data === 200));

    const subs = this.summaryService.subscribe((summary: any) => {
      if (!summary) {
        return;
      }

      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl =
        `http://docs.ceph.com/docs/${releaseName}/mgr/dashboard/` +
        `#enabling-the-embedding-of-grafana-dashboards`;

      setTimeout(() => {
        subs.unsubscribe();
      }, 0);
    });
    this.loading = true;
    this.settingsService.ifSettingConfigured('api/grafana/url', (url) => {
      this.grafanaExist = true;
      this.baseUrl = url + '/d/';
      this.getFrame();
    });
    this.panelStyle = this.styles[this.grafanaStyle];
  }

  setInitialValues() {
    const grafana = this.uiSettings['grafana'];
    this.time = grafana.timepicker;
    this.refresh = grafana.refresh_interval;
    this.loading = false;
  }

  getUrl() {
    this.url =
      this.baseUrl +
      this.uid +
      '/' +
      this.grafanaPath +
      '&refresh=' +
      this.refresh +
      's' +
      this.mode +
      '&' +
      this.time;

    this.grafanaSrc = this.sanitizer.bypassSecurityTrustResourceUrl(this.url);
  }

  getFrame() {
    this.settingRegistryService.getSettingsList().subscribe((data) => {
      data.forEach((d) => (this.uiSettings[d['name']] = d));
      this.setInitialValues();
      this.getUrl();
    });
  }

  onTimepickerChange() {
    if (this.grafanaExist && this.time && this.refresh) {
      this.getUrl();
    }
  }

  reset() {
    this.loading = true;

    if (this.grafanaExist) {
      this.getFrame();
    }
  }

  ngOnChanges() {
    if (this.grafanaExist && this.time && this.refresh) {
      this.getFrame();
    }
  }
}
