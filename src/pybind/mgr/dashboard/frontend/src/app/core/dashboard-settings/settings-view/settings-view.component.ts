import { Component, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { SettingRegistryService } from '../../../shared/api/setting-registry.service';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-settings-view',
  templateUrl: './settings-view.component.html',
  styleUrls: ['./settings-view.component.scss']
})
export class SettingsViewComponent implements OnInit {
  grafana: any;
  settingList: any;
  loading = false;
  error = false;
  selection = new CdTableSelection();
  columns: Array<CdTableColumn> = [];
  tableActions: CdTableAction[];
  permission: Permission;
  uiSettings = {};

  constructor(
    private i18n: I18n,
    private authStorageService: AuthStorageService,
    private settingRegistryService: SettingRegistryService
  ) {
    this.permission = this.authStorageService.getPermissions().user;

    const editAction: CdTableAction = {
      name: this.i18n('Edit'),
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => '/settings/edit/' + encodeURIComponent(this.selection.first().name)
    };

    this.tableActions = [editAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'name',
        flexGrow: 1
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getSettingsList() {
    this.settingRegistryService.getSettingsList().subscribe((data) => {
      data.forEach((d) => (this.uiSettings[d['name']] = d));
      this.settingList = Object.values(this.uiSettings);
    });
  }
}
