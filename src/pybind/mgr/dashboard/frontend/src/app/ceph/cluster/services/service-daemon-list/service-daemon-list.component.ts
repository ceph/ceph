import {
  AfterViewInit,
  Component,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  QueryList,
  TemplateRef,
  ViewChild,
  ViewChildren
} from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { Observable, Subscription } from 'rxjs';

import { CephServiceService } from '../../../../shared/api/ceph-service.service';
import { HostService } from '../../../../shared/api/host.service';
import { OrchestratorService } from '../../../../shared/api/orchestrator.service';
import { TableComponent } from '../../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../../shared/models/cd-table-fetch-data-context';
import { Daemon } from '../../../../shared/models/daemon.interface';

@Component({
  selector: 'cd-service-daemon-list',
  templateUrl: './service-daemon-list.component.html',
  styleUrls: ['./service-daemon-list.component.scss']
})
export class ServiceDaemonListComponent implements OnInit, OnChanges, AfterViewInit, OnDestroy {
  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;

  @ViewChildren('daemonsTable')
  daemonsTableTpls: QueryList<TemplateRef<TableComponent>>;

  @Input()
  serviceName?: string;

  @Input()
  hostname?: string;

  daemons: Daemon[] = [];
  columns: CdTableColumn[] = [];

  hasOrchestrator = false;

  private daemonsTable: TableComponent;
  private daemonsTableTplsSub: Subscription;

  constructor(
    private i18n: I18n,
    private hostService: HostService,
    private cephServiceService: CephServiceService,
    private orchService: OrchestratorService
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
        filterable: true,
        cellTransformation: CellTemplate.truncate,
        customTemplateConfig: {
          length: 12
        }
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
        filterable: true,
        cellTransformation: CellTemplate.truncate,
        customTemplateConfig: {
          length: 12
        }
      },
      {
        name: this.i18n('Version'),
        prop: 'version',
        flexGrow: 1,
        filterable: true
      },
      {
        name: this.i18n('Status'),
        prop: 'status_desc',
        flexGrow: 1,
        filterable: true,
        cellTemplate: this.statusTpl
      },
      {
        name: this.i18n('Last Refreshed'),
        prop: 'last_refresh',
        flexGrow: 2
      }
    ];

    this.orchService.status().subscribe((data: { available: boolean }) => {
      this.hasOrchestrator = data.available;
    });
  }

  ngOnChanges() {
    if (!_.isUndefined(this.daemonsTable)) {
      this.daemonsTable.reloadData();
    }
  }

  ngAfterViewInit() {
    this.daemonsTableTplsSub = this.daemonsTableTpls.changes.subscribe(
      (tableRefs: QueryList<TableComponent>) => {
        this.daemonsTable = tableRefs.first;
      }
    );
  }

  ngOnDestroy() {
    if (this.daemonsTableTplsSub) {
      this.daemonsTableTplsSub.unsubscribe();
    }
  }

  getStatusClass(status: number) {
    return _.get(
      {
        '-1': 'badge-danger',
        '0': 'badge-warning',
        '1': 'badge-success'
      },
      status,
      'badge-dark'
    );
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
