import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { HostService } from '../../../shared/services/host.service';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss']
})
export class HostsComponent implements OnInit {

  columns: Array<object> = [];
  hosts: Array<object> = [];

  @ViewChild('servicesTpl') public servicesTpl: TemplateRef<any>;

  constructor(private hostService: HostService,
              private cephShortVersionPipe: CephShortVersionPipe) { }

  ngOnInit() {
    this.columns = [
      {
        name: 'Hostname',
        prop: 'hostname',
        flexGrow: 1
      },
      {
        name: 'Services',
        prop: 'services',
        flexGrow: 3,
        cellTemplate: this.servicesTpl
      },
      {
        name: 'Version',
        prop: 'ceph_version',
        flexGrow: 1,
        pipe: this.cephShortVersionPipe
      }
    ];
    this.hostService.list().then((resp) => {
      resp.map((host) => {
        host.services.map((service) => {
          service.cdLink = `/perf_counters/${service.type}/${service.id}`;
          return service;
        });
        return host;
      });
      this.hosts = resp;
    });
  }

}
