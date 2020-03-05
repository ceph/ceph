import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { Observable } from 'rxjs';

import { CephServiceService } from '../../../../shared/api/ceph-service.service';
import { HostService } from '../../../../shared/api/host.service';
import { TableComponent } from '../../../../shared/datatable/table/table.component';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../../shared/models/cd-table-fetch-data-context';
import { Daemon } from '../../../../shared/models/daemon.interface';

@Component({
  selector: 'cd-service-daemon-list',
  templateUrl: './service-daemon-list.component.html',
  styleUrls: ['./service-daemon-list.component.scss']
})
export class ServiceDaemonListComponent implements OnInit, OnChanges {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  @ViewChild('lastSeenTpl', { static: true })
  lastSeenTpl: TemplateRef<any>;

  @Input()
  serviceName?: string;

  @Input()
  hostname?: string;

  daemons: Daemon[] = [];
  columns: CdTableColumn[] = [];

  constructor(
    private i18n: I18n,
    private hostService: HostService,
    private cephServiceService: CephServiceService
  ) {}

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Hostname'),
        prop: 'hostname',
        flexGrow: 1,
        filterable: true
      },
      {
        name: this.i18n('Daemon type'),
        prop: 'daemon_type',
        flexGrow: 1,
        filterable: true
      },
      {
        name: this.i18n('Daemon ID'),
        prop: 'daemon_id',
        flexGrow: 1,
        filterable: true
      },
      {
        name: this.i18n('Container ID'),
        prop: 'container_id',
        flexGrow: 3,
        filterable: true
      },
      {
        name: this.i18n('Container Image name'),
        prop: 'container_image_name',
        flexGrow: 3,
        filterable: true
      },
      {
        name: this.i18n('Container Image ID'),
        prop: 'container_image_id',
        flexGrow: 3,
        filterable: true
      },
      {
        name: this.i18n('Version'),
        prop: 'version',
        flexGrow: 1,
        filterable: true
      },
      {
        name: this.i18n('Status'),
        prop: 'status',
        flexGrow: 1,
        filterable: true
      },
      {
        name: this.i18n('Status Description'),
        prop: 'status_desc',
        flexGrow: 1,
        filterable: true
      },
      {
        name: this.i18n('Last Refreshed'),
        prop: 'last_refresh',
        flexGrow: 2
      }
    ];
  }

  ngOnChanges() {
    this.daemons = [];
    this.table.reloadData();
  }

  updateData(daemons: Daemon[]) {
    this.daemons = daemons;
  }

  getDaemons(context: CdTableFetchDataContext) {
    let observable: Observable<Daemon[]>;
    if (this.hostname) {
      observable = this.hostService.getDaemons(this.hostname);
    } else if (this.serviceName) {
      observable = this.cephServiceService.getDaemons(this.serviceName);
    } else {
      this.daemons = [];
      return;
    }
    observable.subscribe(
      (daemons: Daemon[]) => {
        this.daemons = daemons;
      },
      () => {
        this.daemons = [];
        context.error();
      }
    );
  }
}
