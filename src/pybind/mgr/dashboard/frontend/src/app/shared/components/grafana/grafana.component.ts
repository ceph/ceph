import { Component, Input, OnChanges, OnInit } from '@angular/core';
import { DomSanitizer, SafeUrl } from '@angular/platform-browser';

import { SettingsService } from '~/app/shared/api/settings.service';
import { Icons } from '~/app/shared/enum/icons.enum';

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
  datasource: string;
  loading = true;
  styles: Record<string, string> = {};
  dashboardExist = true;
  showMessage = false;
  time: string;
  grafanaTimes: any;
  icons = Icons;
  readonly DEFAULT_TIME: string = 'from=now-1h&to=now';

  @Input()
  type: string;
  @Input()
  grafanaPath: string;
  @Input()
  grafanaStyle: string;
  @Input()
  uid: string;
  @Input()
  title: string;

  constructor(private sanitizer: DomSanitizer, private settingsService: SettingsService) {
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
      three: 'grafana_three',
      four: 'grafana_four'
    };

    this.datasource = this.type === 'metrics' ? 'Dashboard1' : 'Loki';

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
      .subscribe((data: any) => (this.dashboardExist = data === 200 || data === 401)); // 401 because grafana API shows unauthorized when anonymous access is disabled
    if (this.type === 'metrics') {
      this.url = `${this.baseUrl}${this.uid}/${this.grafanaPath}&refresh=2s&var-datasource=${this.datasource}${this.mode}&${this.time}`;
    } else {
      this.url = `${this.baseUrl.slice(0, -2)}${this.grafanaPath}orgId=1&left={"datasource": "${
        this.datasource
      }", "queries": [{"refId": "A"}], "range": {"from": "now-1h", "to": "now"}}${this.mode}`;
    }
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
