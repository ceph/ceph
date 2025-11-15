import { Component, ViewChild, OnInit } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { BehaviorSubject } from 'rxjs';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { ClusterRow, Peer } from '../cephfs-mirroring.model';

const BASE_URL = 'cephfs/mirroring';

@Component({
  selector: 'cd-cephfs-mirroring-detail-list',
  templateUrl: './cephfs-mirroring-detail-list.component.html',
  styleUrls: ['./cephfs-mirroring-detail-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class CephfsMirroringDetailListComponent implements OnInit {
  @ViewChild('table', { static: true }) table: TableComponent;
  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;
  selection = new CdTableSelection();
  modalRef: NgbModalRef;
  subject$ = new BehaviorSubject<ClusterRow[]>([]);
  mirroringData$ = this.subject$.asObservable();

  dummyData = {
    rados_inst: '192.168.100.101:0/1641747406',
    peers: {
      'f02dfb52-520a-4a64-abee-a51c9fb854ef': {
        remote: {
          client_name: 'client.mirror_remote',
          cluster_name: 'site-remote',
          fs_name: 'testData'
        },
        local: {
          cluster_name: 'site-local'
        }
      }
    },
    snap_dirs: {
      dir_count: 1
    }
  };

  constructor(public actionLabels: ActionLabelsI18n) {}

  ngOnInit() {
    this.columns = [
      { name: $localize`Remote Cluster Name`, prop: 'remote_cluster_name', flexGrow: 2 },
      { name: $localize`Local Cluster Name`, prop: 'local_cluster_name', flexGrow: 2 },
      { name: $localize`Filesystem Name`, prop: 'fs_name', flexGrow: 2 },
      { name: $localize`Client Name`, prop: 'client_name', flexGrow: 2 },
      { name: $localize`RADOS Instance`, prop: 'rados_inst', flexGrow: 2 },
      { name: $localize`Directory Count`, prop: 'dir_count', flexGrow: 1 }
    ];

    this.loadMirroringData();
  }

  loadMirroringData(): void {
    const rows = Object.values(this.dummyData.peers).map((peerData: Peer) => ({
      remote_cluster_name: peerData.remote.cluster_name,
      local_cluster_name: peerData.local.cluster_name || '-',
      fs_name: peerData.remote.fs_name,
      client_name: peerData.remote.client_name,
      rados_inst: this.dummyData.rados_inst,
      dir_count: this.dummyData.snap_dirs.dir_count
    }));

    this.subject$.next(rows);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
