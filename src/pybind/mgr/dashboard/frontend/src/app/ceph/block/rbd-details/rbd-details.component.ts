import { Component, Input, OnChanges, TemplateRef, ViewChild } from '@angular/core';

import { NgbNav } from '@ng-bootstrap/ng-bootstrap';

import { RbdFormModel } from '../rbd-form/rbd-form.model';

@Component({
  selector: 'cd-rbd-details',
  templateUrl: './rbd-details.component.html',
  styleUrls: ['./rbd-details.component.scss']
})
export class RbdDetailsComponent implements OnChanges {
  @Input()
  selection: RbdFormModel;
  @Input()
  images: any;

  @ViewChild('poolConfigurationSourceTpl', { static: true })
  poolConfigurationSourceTpl: TemplateRef<any>;

  @ViewChild(NgbNav, { static: true })
  nav: NgbNav;

  rbdDashboardUrl: string;

  ngOnChanges() {
    if (this.selection) {
      this.rbdDashboardUrl = `rbd-details?var-pool=${this.selection['pool_name']}&var-image=${this.selection['name']}`;
    }
  }
}
