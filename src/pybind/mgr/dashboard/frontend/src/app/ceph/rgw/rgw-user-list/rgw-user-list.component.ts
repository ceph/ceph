import { Component } from '@angular/core';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-rgw-user-list',
  templateUrl: './rgw-user-list.component.html',
  styleUrls: ['./rgw-user-list.component.scss']
})
export class RgwUserListComponent {

  columns: CdTableColumn[] = [];
  users: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(private rgwUserService: RgwUserService) {
    this.columns = [
      {
        name: 'Username',
        prop: 'user_id',
        flexGrow: 1
      },
      {
        name: 'Full name',
        prop: 'display_name',
        flexGrow: 1
      },
      {
        name: 'Email address',
        prop: 'email',
        flexGrow: 1
      },
      {
        name: 'Suspended',
        prop: 'suspended',
        flexGrow: 1,
        cellTransformation: CellTemplate.checkIcon
      },
      {
        name: 'Max. buckets',
        prop: 'max_buckets',
        flexGrow: 1
      }
    ];
  }

  getUserList() {
    this.rgwUserService.list()
      .subscribe((resp: object[]) => {
        this.users = resp;
      });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
