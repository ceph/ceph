import { Component, Input, OnChanges, OnInit } from '@angular/core';

import { DomSanitizer } from '@angular/platform-browser';
import { SafeUrl } from '@angular/platform-browser';

import { SettingsService } from '../../../shared/api/settings.service';

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
  grafanaUrl: any;

  constructor(private sanitizer: DomSanitizer, private settingsService: SettingsService) {}

  ngOnInit() {
    this.styles = {
      one: 'grafana_one',
      two: 'grafana_two',
      three: 'grafana_three'
    };
    this.settingsService.getGrafanaApiUrl().subscribe((data: any) => {
      this.grafanaUrl = data.value;
      if (this.grafanaUrl === '') {
        this.grafanaExist = false;
        return;
      } else {
        this.getFrame();
      }
    });
    this.panelStyle = this.styles[this.grafanaStyle];
  }

  getFrame() {
    this.baseUrl = this.grafanaUrl + '/d/';
    this.grafanaExist = true;
    this.loading = false;
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
