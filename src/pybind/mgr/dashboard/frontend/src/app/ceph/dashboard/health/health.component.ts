import { Component, OnDestroy, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { DashboardService } from '../../../shared/api/dashboard.service';

import {InfoCard} from "../info-card/info-card";

import {MonSummaryPipe} from "../mon-summary.pipe";
import {OsdSummaryPipe} from "../osd-summary.pipe";
import {HealthColorPipe} from "../../../shared/pipes/health-color.pipe";
import {InfoCardAdditionalInfo} from "../info-card/info-card-additional-info";
import {MdsSummaryPipe} from "../mds-summary.pipe";

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  contentData: any;
  interval: number;

  healthStatusCard: InfoCard;
  monitorsCard: InfoCard;
  osdCard: InfoCard;
  mdsCard: InfoCard;

  constructor(
    private dashboardService: DashboardService,
    private monSummaryPipe: MonSummaryPipe,
    private osdSummaryPipe: OsdSummaryPipe,
    private healthColorPipe: HealthColorPipe,
    private mdsSummaryPipe: MdsSummaryPipe,
    ) {}

  ngOnInit() {
    this.getInfo();
    this.interval = window.setInterval(() => {
      this.getInfo();
    }, 5000);
  }

  ngOnDestroy() {
    clearInterval(this.interval);
  }

  private getInfo() {
    this.dashboardService.getHealth().subscribe((data: any) => {
      this.contentData = data;

      if (this.contentData) {
        this.initInfoCards();
      }
    });
  }

  prepareRawUsage(chart, data) {
    let rawUsageChartColor;

    const rawUsageText =
      Math.round(100 * (data.df.stats.total_used_bytes / data.df.stats.total_bytes)) + '%';

    if (data.df.stats.total_used_bytes / data.df.stats.total_bytes >= data.osd_map.full_ratio) {
      rawUsageChartColor = '#ff0000';
    } else if (
      data.df.stats.total_used_bytes / data.df.stats.total_bytes >=
      data.osd_map.backfillfull_ratio
    ) {
      rawUsageChartColor = '#ff6600';
    } else if (
      data.df.stats.total_used_bytes / data.df.stats.total_bytes >=
      data.osd_map.nearfull_ratio
    ) {
      rawUsageChartColor = '#ffc200';
    } else {
      rawUsageChartColor = '#00bb00';
    }

    chart.dataset[0].data = [data.df.stats.total_used_bytes, data.df.stats.total_avail_bytes];
    chart.options.center_text = rawUsageText;
    chart.colors = [{ backgroundColor: [rawUsageChartColor, '#424d52'] }];
    chart.labels = ['Raw Used', 'Raw Available'];
  }

  preparePoolUsage(chart, data) {
    const colors = [
      '#3366CC',
      '#109618',
      '#990099',
      '#3B3EAC',
      '#0099C6',
      '#DD4477',
      '#66AA00',
      '#B82E2E',
      '#316395',
      '#994499',
      '#22AA99',
      '#AAAA11',
      '#6633CC',
      '#E67300',
      '#8B0707',
      '#329262',
      '#5574A6',
      '#FF9900',
      '#DC3912',
      '#3B3EAC'
    ];

    const poolLabels = [];
    const poolData = [];

    _.each(data.df.pools, (pool, i) => {
      poolLabels.push(pool['name']);
      poolData.push(pool['stats']['bytes_used']);
    });

    chart.dataset[0].data = poolData;
    chart.colors = [{ backgroundColor: colors }];
    chart.labels = poolLabels;
  }

  private initInfoCards() {
    this.initHealthStatusCard();
    this.initMonitorsCard();
    this.initOsdCard();
    this.initMdsCard();
  }

  private initHealthStatusCard() {
    this.healthStatusCard = new InfoCard('HEALTH');
    this.healthStatusCard.info = this.contentData.health.status;
    this.healthStatusCard.infoStyle = this.healthColorPipe.transform(this.healthStatusCard.info);

    let additionalInfo: InfoCardAdditionalInfo[] = [];
    for (let check of this.contentData.health.checks) {
      let additionalRow = new InfoCardAdditionalInfo(check.summary.message);
      additionalRow.style = this.healthColorPipe.transform(check.severity);

      additionalInfo.push(additionalRow);
    }

    this.healthStatusCard.additionalInfo = additionalInfo;
  }

  private initMonitorsCard() {
    if (this.contentData.mon_status) {
      this.monitorsCard = new InfoCard('MONITORS');
      this.monitorsCard.titleLink = '/monitor/';
      this.monitorsCard.titleImageClass = 'fa fa-database fa-fw';
      this.monitorsCard.info = this.monSummaryPipe.transform(this.contentData.mon_status);
      this.monitorsCard.infoClass = 'media-text';
    }
  }

  private initOsdCard() {
    if (this.contentData.osd_map) {
      this.osdCard = new InfoCard('OSDS');
      this.osdCard.titleLink = '/osd/';
      this.osdCard.titleImageClass = 'fa fa-hdd-o fa-fw';
      this.osdCard.info = this.osdSummaryPipe.transform(this.contentData.osd_map);
      this.osdCard.infoClass = 'media-text';
    }
  }

  private initMdsCard() {
    if (this.contentData.fs_map) {
      this.mdsCard = new InfoCard('METADATA SERVERS');
      this.mdsCard.titleImageClass = 'fa fa-folder fa-fw';
      this.mdsCard.info = this.mdsSummaryPipe.transform(this.contentData.fs_map);
      this.mdsCard.infoClass = 'media-text';
    }
  }
}
