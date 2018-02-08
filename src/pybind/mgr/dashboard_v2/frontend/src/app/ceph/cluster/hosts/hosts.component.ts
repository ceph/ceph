import { Component, OnInit } from '@angular/core';

import { ServiceListPipe } from '../service-list.pipe';

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

  constructor(private hostService: HostService,
              cephShortVersionPipe: CephShortVersionPipe,
              serviceListPipe: ServiceListPipe) {
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
        pipe: serviceListPipe
      },
      {
        name: 'Version',
        prop: 'ceph_version',
        flexGrow: 1,
        pipe: cephShortVersionPipe
      }
    ];
  }

  ngOnInit() {
    this.hostService.list().then((resp) => {
      this.hosts = resp;
    });
  }

}
