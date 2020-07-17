import { Component, Input, OnChanges, OnInit } from '@angular/core';
import { DomSanitizer, SafeUrl } from '@angular/platform-browser';

import { Icons } from '../../../shared/enum/icons.enum';
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
  port: number;
  baseUrl: any;
  panelStyle: any;
  grafanaExist = false;
  mode = '&kiosk';
  loading = true;
  styles: Record<string, string> = {};
  dashboardExist = true;
  time: string;
  grafanaTimes: any;
  icons = Icons;
  readonly DEFAULT_TIME: string = 'from=now-1h&to=now';

  @Input()
  grafanaPath: string;
  @Input()
  grafanaStyle: string;
  @Input()
  uid: string;

  docsUrl: string;

  constructor(
    private summaryService: SummaryService,
    private sanitizer: DomSanitizer,
    private settingsService: SettingsService,
    private cephReleaseNamePipe: CephReleaseNamePipe
  ) {
    this.grafanaTimes = [
      {
        name: $localize`Last 5 minutes`,
        value: 'from=now-5m&to=now'
      },
      {
        name: $localize`Last 15 minutes`,
        value: 'from=now-15m&to=now'
      },
      {
        name: $localize`Last 30 minutes`,
        value: 'from=now-30m&to=now'
      },
      {
        name: $localize`Last 1 hour (Default)`,
        value: 'from=now-1h&to=now'
      },
      {
        name: $localize`Last 3 hours`,
        value: 'from=now-3h&to=now'
      },
      {
        name: $localize`Last 6 hours`,
        value: 'from=now-6h&to=now'
      },
      {
        name: $localize`Last 12 hours`,
        value: 'from=now-12h&to=now'
      },
      {
        name: $localize`Last 24 hours`,
        value: 'from=now-24h&to=now'
      },
      {
        name: $localize`Yesterday`,
        value: 'from=now-1d%2Fd&to=now-1d%2Fd'
      },
      {
        name: $localize`Today`,
        value: 'from=now%2Fd&to=now%2Fd'
      },
      {
        name: $localize`Today so far`,
        value: 'from=now%2Fd&to=now'
      },
      {
        name: $localize`Day before yesterday`,
        value: 'from=now-2d%2Fd&to=now-2d%2Fd'
      },
      {
        name: $localize`Last 2 days`,
        value: 'from=now-2d&to=now'
      },
      {
        name: $localize`This day last week`,
        value: 'from=now-7d%2Fd&to=now-7d%2Fd'
      },
      {
        name: $localize`Previous week`,
        value: 'from=now-1w%2Fw&to=now-1w%2Fw'
      },
      {
        name: $localize`This week`,
        value: 'from=now%2Fw&to=now%2Fw'
      },
      {
        name: $localize`This week so far`,
        value: 'from=now%2Fw&to=now'
      },
      {
        name: $localize`Last 7 days`,
        value: 'from=now-7d&to=now'
      },
      {
        name: $localize`Previous month`,
        value: 'from=now-1M%2FM&to=now-1M%2FM'
      },
      {
        name: $localize`This month`,
        value: 'from=now%2FM&to=now%2FM'
      },
      {
        name: $localize`This month so far`,
        value: 'from=now%2FM&to=now'
      },
      {
        name: $localize`Last 30 days`,
        value: 'from=now-30d&to=now'
      },
      {
        name: $localize`Last 90 days`,
        value: 'from=now-90d&to=now'
      },
      {
        name: $localize`Last 6 months`,
        value: 'from=now-6M&to=now'
      },
      {
        name: $localize`Last 1 year`,
        value: 'from=now-1y&to=now'
      },
      {
        name: $localize`Previous year`,
        value: 'from=now-1y%2Fy&to=now-1y%2Fy'
      },
      {
        name: $localize`This year`,
        value: 'from=now%2Fy&to=now%2Fy'
      },
      {
        name: $localize`This year so far`,
        value: 'from=now%2Fy&to=now'
      },
      {
        name: $localize`Last 2 years`,
        value: 'from=now-2y&to=now'
      },
      {
        name: $localize`Last 5 years`,
        value: 'from=now-5y&to=now'
      }
    ];
  }

  ngOnInit() {
    this.time = this.DEFAULT_TIME;
    this.styles = {
      one: 'grafana_one',
      two: 'grafana_two',
      three: 'grafana_three'
    };

    this.summaryService.subscribeOnce((summary) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl =
        `http://docs.ceph.com/docs/${releaseName}/mgr/dashboard/` +
        `#enabling-the-embedding-of-grafana-dashboards`;
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
    this.settingsService
      .validateGrafanaDashboardUrl(this.uid)
      .subscribe((data: any) => (this.dashboardExist = data === 200));
    this.url =
      this.baseUrl +
      this.uid +
      '/' +
      this.grafanaPath +
      '&refresh=2s' +
      this.mode +
      '&' +
      this.time;
    this.grafanaSrc = this.sanitizer.bypassSecurityTrustResourceUrl(this.url);
  }

  onTimepickerChange() {
    if (this.grafanaExist) {
      this.getFrame();
    }
  }

  reset() {
    this.time = this.DEFAULT_TIME;
    if (this.grafanaExist) {
      this.getFrame();
    }
  }

  ngOnChanges() {
    if (this.grafanaExist) {
      this.getFrame();
    }
  }
}
