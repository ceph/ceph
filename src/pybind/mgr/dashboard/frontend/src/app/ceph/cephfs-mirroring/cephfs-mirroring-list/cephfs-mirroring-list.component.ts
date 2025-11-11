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
import { Cluster, MirroringRow } from '../cephfs-mirroring.model';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

const BASE_URL = 'cephfs/mirroring';

@Component({
  selector: 'cd-cephfs-mirroring-list',
  templateUrl: './cephfs-mirroring-list.component.html',
  styleUrls: ['./cephfs-mirroring-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class CephfsMirroringListComponent implements OnInit {
  @ViewChild('table', { static: true }) table: TableComponent;
  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;
  selection = new CdTableSelection();
  modalRef: NgbModalRef;
  subject$ = new BehaviorSubject<MirroringRow[]>([]);
  mirroringData$ = this.subject$.asObservable();

  dummyData: Record<string, Cluster> = {
    '/dnyaneetest': {
      state: 'idle',
      last_synced_snap: {
        id: 6,
        name: 'snap1',
        sync_duration: 0,
        sync_time_stamp: '274900.558797s',
        sync_bytes: 0
      },
      snaps_synced: 1,
      snaps_deleted: 0,
      snaps_renamed: 0
    }
  };

  constructor(public actionLabels: ActionLabelsI18n) {}

  ngOnInit() {
    this.columns = [
      { name: $localize`Subvolume / Directory`, prop: 'snapshot', flexGrow: 2 },
      {
        name: $localize`State`,
        prop: 'state',
        flexGrow: 1,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          map: {
            failed: { class: 'tag-danger' },
            idle: { class: 'tag-warning' },
            syncing: { class: 'tag-success' }
          }
        }
      },
      { name: $localize`Last Snap`, prop: 'last_snap', flexGrow: 2 },
      { name: $localize`Duration`, prop: 'duration', flexGrow: 2 },
      { name: $localize`Last Sync Time`, prop: 'last_sync_time', flexGrow: 2 },
      { name: $localize`Progress`, prop: 'progress', flexGrow: 2 }
    ];

    this.loadMirroringData();
  }

  loadMirroringData(): void {
    const rows: MirroringRow[] = Object.entries(this.dummyData).map(
      ([path, data]: [string, Cluster]) => {
        const timeInfo = data.last_synced_snap
          ? this.syncTimeInfo(data.last_synced_snap.sync_time_stamp)
          : { duration: '-', localTime: '-' };

        return {
          snapshot: path,
          state: data.state,
          last_snap: data.last_synced_snap?.name || '-',
          duration: timeInfo.duration,
          last_sync_time: timeInfo.localTime,
          progress: `${data.snaps_synced} synced / ${data.snaps_deleted} deleted`
        };
      }
    );

    this.subject$.next(rows);
  }

  syncTimeInfo(durationStr: string) {
    if (typeof durationStr !== 'string' || !durationStr.endsWith('s')) {
      throw new Error('Invalid duration string: ' + durationStr);
    }

    const totalSeconds = parseFloat(durationStr.slice(0, -1));
    if (isNaN(totalSeconds)) {
      throw new Error('Cannot parse seconds from: ' + durationStr);
    }

    const secondsInMinute = 60;
    const secondsInHour = 3600;
    const secondsInDay = 86400;

    const days = Math.floor(totalSeconds / secondsInDay);
    const hours = Math.floor((totalSeconds % secondsInDay) / secondsInHour);
    const minutes = Math.floor((totalSeconds % secondsInHour) / secondsInMinute);
    const seconds = (totalSeconds % secondsInMinute).toFixed(3);

    const now = new Date();
    const syncDate = new Date(now.getTime() - totalSeconds * 1000);
    const localTime = syncDate.toLocaleString();

    return {
      duration: `${days}d ${hours}h ${minutes}m ${seconds}s ago`,
      localTime
    };
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
