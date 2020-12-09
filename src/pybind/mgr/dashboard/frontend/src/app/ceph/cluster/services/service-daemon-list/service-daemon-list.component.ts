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

import _ from 'lodash';
import { Observable, Subscription } from 'rxjs';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Daemon } from '~/app/shared/models/daemon.interface';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';

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
  showDocPanel = false;

  private daemonsTable: TableComponent;
  private daemonsTableTplsSub: Subscription;

  constructor(
    private hostService: HostService,
    private cephServiceService: CephServiceService,
    private orchService: OrchestratorService,
    private relativeDatePipe: RelativeDatePipe
  ) {}

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Daemon type`,
        prop: 'daemon_type',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Daemon ID`,
        prop: 'daemon_id',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Container ID`,
        prop: 'container_id',
        flexGrow: 3,
        filterable: true,
        cellTransformation: CellTemplate.truncate,
        customTemplateConfig: {
          length: 12
        }
      },
      {
        name: $localize`Container Image name`,
        prop: 'container_image_name',
        flexGrow: 3,
        filterable: true
      },
      {
        name: $localize`Container Image ID`,
        prop: 'container_image_id',
        flexGrow: 3,
        filterable: true,
        cellTransformation: CellTemplate.truncate,
        customTemplateConfig: {
          length: 12
        }
      },
      {
        name: $localize`Version`,
        prop: 'version',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Status`,
        prop: 'status_desc',
        flexGrow: 1,
        filterable: true,
        cellTemplate: this.statusTpl
      },
      {
        name: $localize`Last Refreshed`,
        prop: 'last_refresh',
        pipe: this.relativeDatePipe,
        flexGrow: 2
      }
    ];

    this.orchService.status().subscribe((data: { available: boolean }) => {
      this.hasOrchestrator = data.available;
      this.showDocPanel = !data.available;
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

  getStatusClass(row: Daemon): string {
    return _.get(
      {
        '-1': 'badge-danger',
        '0': 'badge-warning',
        '1': 'badge-success'
      },
      row.status,
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
