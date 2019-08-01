import { Component, Input, OnInit } from '@angular/core';

import { CdIndividualConfig } from '../../models/cd-notification';

@Component({
  selector: 'cd-toast-footer',
  templateUrl: './toast-footer.component.html',
  styleUrls: ['./toast-footer.component.scss']
})
export class ToastFooterComponent implements OnInit {
  @Input()
  config: CdIndividualConfig;

  public timestamp: string;
  public applicationClass: string;
  public application: string;
  private classes = {
    Ceph: 'ceph-icon',
    Prometheus: 'prometheus-icon'
  };

  constructor() {}

  ngOnInit() {
    this.timestamp = this.config.timestamp;
    this.applicationClass = this.classes[this.config.application];
    this.application = this.config.application;
  }
}
