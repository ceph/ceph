import { Component, Input, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { OsdService } from '../osd.service';

@Component({
  selector: 'cd-osd-details',
  templateUrl: './osd-details.component.html',
  styleUrls: ['./osd-details.component.scss']
})
export class OsdDetailsComponent implements OnInit {
  @Input() selected?: any[];

  constructor(private osdService: OsdService) {}

  ngOnInit() {
    _.each(this.selected, (osd) => {
      this.refresh(osd);
      osd.autoRefresh = () => {
        this.refresh(osd);
      };
    });
  }

  refresh(osd: any) {
    this.osdService.getDetails(osd.tree.id).subscribe((data: any) => {
      osd.details = data;
      if (!_.isObject(data.histogram)) {
        osd.histogram_failed = data.histogram;
        osd.details.histogram = undefined;
      }
      osd.loaded = true;
    });
  }

}
