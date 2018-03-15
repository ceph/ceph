import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { HostService } from '../../../shared/services/host.service';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss']
})
export class HostsComponent implements OnInit {

  columns: Array<CdTableColumn> = [];
  hosts: Array<object> = [];
  isLoadingHosts = false;

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
  }

  getHosts() {
    if (this.isLoadingHosts) {
      return;
    }
    this.isLoadingHosts = true;
    this.hostService.list().then((resp) => {
      resp.map((host) => {
        host.services.map((service) => {
          service.cdLink = `/perf_counters/${service.type}/${service.id}`;
          return service;
        });
        return host;
      });
      this.hosts = resp;
      this.isLoadingHosts = false;
    }).catch(() => {
      this.isLoadingHosts = false;
    });
  }
}
