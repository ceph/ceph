import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { NFSCluster } from '../models/nfs-cluster-config';
import { getFsalFromRoute, getPathfromFsal } from '../utils';

@Component({
  selector: 'cd-nfs-cluster',
  templateUrl: './nfs-cluster.component.html',
  styleUrls: ['./nfs-cluster.component.scss'],
  standalone: false
})
export class NfsClusterComponent implements OnInit {
  @ViewChild('hostnameTpl', { static: true })
  hostnameTpl: TemplateRef<any>;

  @ViewChild('ipAddrTpl', { static: true })
  ipAddrTpl: TemplateRef<any>;

  @ViewChild('virtualIpTpl', { static: true })
  virtualIpTpl: TemplateRef<any>;

  columns: CdTableColumn[] = [];
  orchStatus: OrchestratorStatus;
  clusters$: Observable<NFSCluster[]>;
  subject = new BehaviorSubject<NFSCluster[]>([]);

  constructor(
    private nfsService: NfsService,
    private orchService: OrchestratorService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.orchService.status().subscribe((status: OrchestratorStatus) => {
      this.orchStatus = status;
    });
    this.clusters$ = this.subject.pipe(
      switchMap(() => this.nfsService.listClusters(true).pipe(catchError(() => of([]))))
    );
    const nfsBasePath = `/${getPathfromFsal(getFsalFromRoute(this.router.url))}/nfs/cluster`;
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1,
        cellTransformation: CellTemplate.redirect,
        customTemplateConfig: {
          redirectLink: [nfsBasePath, '::prop', 'overview']
        }
      },
      {
        name: $localize`Hostnames`,
        prop: 'backend',
        flexGrow: 2,
        cellTemplate: this.hostnameTpl
      },
      {
        name: $localize`IP Address`,
        prop: 'backend',
        flexGrow: 2,
        cellTemplate: this.ipAddrTpl
      },
      {
        name: $localize`Virtual IP Address`,
        prop: 'virtual_ip',
        flexGrow: 1,
        cellTemplate: this.virtualIpTpl
      }
    ];
  }

  loadData() {
    this.subject.next([]);
  }
}
