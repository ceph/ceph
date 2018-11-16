import { Component } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { RgwDaemonService } from '../../../shared/api/rgw-daemon.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';

@Component({
  selector: 'cd-rgw-daemon-list',
  templateUrl: './rgw-daemon-list.component.html',
  styleUrls: ['./rgw-daemon-list.component.scss']
})
export class RgwDaemonListComponent {
  columns: CdTableColumn[] = [];
  daemons: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private rgwDaemonService: RgwDaemonService,
    cephShortVersionPipe: CephShortVersionPipe,
    private i18n: I18n
  ) {
    this.columns = [
      {
        name: this.i18n('ID'),
        prop: 'id',
        flexGrow: 2
      },
      {
        name: this.i18n('Hostname'),
        prop: 'server_hostname',
        flexGrow: 2
      },
      {
        name: this.i18n('Version'),
        prop: 'version',
        flexGrow: 1,
        pipe: cephShortVersionPipe
      }
    ];
  }

  getDaemonList(context: CdTableFetchDataContext) {
    this.rgwDaemonService.list().subscribe(
      (resp: object[]) => {
        this.daemons = resp;
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
