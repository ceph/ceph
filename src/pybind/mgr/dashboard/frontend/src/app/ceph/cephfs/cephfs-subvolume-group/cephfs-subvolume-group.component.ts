import { Component, Input, OnInit, ViewChild } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError } from 'rxjs/operators';

import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CephfsSubvolumeGroup } from '~/app/shared/models/cephfs-subvolume-group.model';

@Component({
  selector: 'cd-cephfs-subvolume-group',
  templateUrl: './cephfs-subvolume-group.component.html',
  styleUrls: ['./cephfs-subvolume-group.component.scss']
})
export class CephfsSubvolumeGroupComponent implements OnInit {
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

  @Input()
  fsName: any;

  columns: CdTableColumn[];
  context: CdTableFetchDataContext;
  selection = new CdTableSelection();

  subvolumeGroup$: Observable<CephfsSubvolumeGroup[]>;

  constructor(private cephfsSubvolumeGroup: CephfsSubvolumeGroupService) {}

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 0.6,
        cellTransformation: CellTemplate.bold
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
    this.subvolumeGroup$ = this.cephfsSubvolumeGroup.get(this.fsName).pipe(
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
