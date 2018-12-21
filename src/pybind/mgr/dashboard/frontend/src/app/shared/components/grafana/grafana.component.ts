import { Component, Input, OnChanges, OnInit } from '@angular/core';

import { DomSanitizer } from '@angular/platform-browser';
import { SafeUrl } from '@angular/platform-browser';

import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { SummaryService } from '../../../shared/services/summary.service';
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
  dashboardPath: string;
  port: number;
  baseUrl: any;
  panelStyle: any;
  grafanaExist = false;
  mode = '&kiosk';
  modeFlag = false;
  modeText = 'Change time selection';
  loading = true;
  styles = {};

  @Input()
  grafanaPath: string;
  @Input()
  grafanaStyle: string;
  docsUrl: string;

  constructor(
    private summaryService: SummaryService,
    private sanitizer: DomSanitizer,
    private settingsService: SettingsService,
    private cephReleaseNamePipe: CephReleaseNamePipe
  ) {}

  ngOnInit() {
    this.styles = {
      one: 'grafana_one',
      two: 'grafana_two',
      three: 'grafana_three'
    };

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

    this.settingsService.ifSettingConfigured('api/grafana/url', (url) => {
      this.grafanaExist = true;
      this.loading = false;
      this.baseUrl = url + '/d/';
      this.getFrame();
    });
    this.panelStyle = this.styles[this.grafanaStyle];
  }

  getFrame() {
    this.url = this.baseUrl + this.grafanaPath + '&refresh=2s' + this.mode;
    this.grafanaSrc = this.sanitizer.bypassSecurityTrustResourceUrl(this.url);
  }

  timePickerToggle() {
    this.modeFlag = true;
    this.mode = this.mode ? '' : '&kiosk';
    if (this.modeText === 'Return to default') {
      this.modeText = 'Change time selection';
      this.reset();
    } else {
      this.modeText = 'Return to default';
    }
    this.getFrame();
    this.modeFlag = false;
  }

  reset() {
    this.mode = '&kiosk';
    this.modeText = 'Change time selection';
    this.getFrame();
  }

  ngOnChanges(changes) {
    this.getFrame();
  }
}
