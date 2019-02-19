import { Component, Input, OnChanges, ViewChild } from '@angular/core';

import { TabsetComponent } from 'ngx-bootstrap/tabs';

import { GrafanaTimesConstants } from '../../../shared/constants/grafanaTimes.constants';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-setting-details',
  templateUrl: './setting-details.component.html',
  styleUrls: ['./setting-details.component.scss']
})
export class SettingDetailsComponent implements OnChanges {
  @Input()
  selection: CdTableSelection;
  selectedItem: any;

  @ViewChild(TabsetComponent)
  tabsetChild: TabsetComponent;

  constructor() {}

  _setGrafanaLabels(data) {
    const time = GrafanaTimesConstants.grafanaTimeData.find((o) => o.value === data.timepicker);
    this.selectedItem['Name'] = data.name;
    this.selectedItem['Refresh Interval'] = data.refresh_interval;
    this.selectedItem['Default timepicker'] = time.name;
  }

  ngOnChanges() {
    this.selectedItem = {};
    if (this.selection.hasSingleSelection) {
      if (this.selection.first().name === 'grafana') {
        this._setGrafanaLabels(this.selection.first());
      }
    }
  }
}
