import { Component, Input, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { OsdService } from '../osd.service';

@Component({
  selector: 'cd-osd-details',
  templateUrl: './osd-details.component.html',
  styleUrls: ['./osd-details.component.scss']
})
export class OsdDetailsComponent implements OnInit {
  osd: any;

  @Input() selected?: any[] = [];

  constructor(private osdService: OsdService) {}

  ngOnInit() {
    this.osd = {
      loaded: false
    };
    if (this.selected.length > 0) {
      this.osd = this.selected[0];
      this.osd.autoRefresh = () => {
        this.refresh();
      };
      this.refresh();
    }
  }

  refresh() {
    this.osdService.getDetails(this.osd.tree.id)
      .subscribe((data: any) => {
        this.osd.details = data;
        if (!_.isObject(data.histogram)) {
          this.osd.histogram_failed = data.histogram;
          this.osd.details.histogram = undefined;
        }
        this.osd.loaded = true;
      });
  }
}
