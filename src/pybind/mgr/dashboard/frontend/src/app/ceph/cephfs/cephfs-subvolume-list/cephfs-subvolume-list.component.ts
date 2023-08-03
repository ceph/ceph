import { Component, Input, OnChanges, OnInit, ViewChild } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CephfsSubvolume } from '~/app/shared/models/cephfs-subvolume.model';

@Component({
  selector: 'cd-cephfs-subvolume-list',
  templateUrl: './cephfs-subvolume-list.component.html',
  styleUrls: ['./cephfs-subvolume-list.component.scss']
})
export class CephfsSubvolumeListComponent implements OnInit, OnChanges {
  @ViewChild('quotaUsageTpl', { static: true })
  quotaUsageTpl: any;

  @ViewChild('typeTpl', { static: true })
  typeTpl: any;

  @ViewChild('modeToHumanReadableTpl', { static: true })
  modeToHumanReadableTpl: any;

  @ViewChild('nameTpl', { static: true })
  nameTpl: any;

  @ViewChild('quotaSizeTpl', { static: true })
  quotaSizeTpl: any;

  @Input() fsName: string;

  columns: CdTableColumn[] = [];
  context: CdTableFetchDataContext;
  selection = new CdTableSelection();
  icons = Icons;

  subVolumes$: Observable<CephfsSubvolume[]>;

  constructor(private cephfsSubVolume: CephfsSubvolumeService) {}

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1,
        cellTemplate: this.nameTpl
      },
      {
        name: $localize`Data Pool`,
        prop: 'info.data_pool',
        flexGrow: 0.7,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          class: 'badge-background-primary'
        }
      },
      {
        name: $localize`Usage`,
        prop: 'info.bytes_pcent',
        flexGrow: 0.7,
        cellTemplate: this.quotaUsageTpl,
        cellClass: 'text-right'
      },
      {
        name: $localize`Path`,
        prop: 'info.path',
        flexGrow: 1,
        cellTransformation: CellTemplate.path
      },
      {
        name: $localize`Mode`,
        prop: 'info.mode',
        flexGrow: 0.5,
        cellTemplate: this.modeToHumanReadableTpl
      },
      {
        name: $localize`Created`,
        prop: 'info.created_at',
        flexGrow: 0.5,
        cellTransformation: CellTemplate.timeAgo
      }
    ];
  }

  ngOnChanges() {
    this.subVolumes$ = this.cephfsSubVolume.get(this.fsName).pipe(
      catchError(() => {
        this.context.error();
        return of(null);
      })
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
