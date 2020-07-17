import { Component, OnInit } from '@angular/core';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { CdDatePipe } from '../../../shared/pipes/cd-date.pipe';

@Component({
  selector: 'cd-cephfs-list',
  templateUrl: './cephfs-list.component.html',
  styleUrls: ['./cephfs-list.component.scss']
})
export class CephfsListComponent extends ListWithDetails implements OnInit {
  columns: CdTableColumn[];
  filesystems: any = [];
  selection = new CdTableSelection();

  constructor(private cephfsService: CephfsService, private cdDatePipe: CdDatePipe) {
    super();
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'mdsmap.fs_name',
        flexGrow: 2
      },
      {
        name: $localize`Created`,
        prop: 'mdsmap.created',
        flexGrow: 2,
        pipe: this.cdDatePipe
      },
      {
        name: $localize`Enabled`,
        prop: 'mdsmap.enabled',
        flexGrow: 1,
        cellTransformation: CellTemplate.checkIcon
      }
    ];
  }

  loadFilesystems(context: CdTableFetchDataContext) {
    this.cephfsService.list().subscribe(
      (resp: any[]) => {
        this.filesystems = resp;
      },
      () => {
        context.error();
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
