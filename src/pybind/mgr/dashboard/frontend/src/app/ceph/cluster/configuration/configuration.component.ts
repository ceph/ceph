import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { ConfigurationService } from '../../../shared/api/configuration.service';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-configuration',
  templateUrl: './configuration.component.html',
  styleUrls: ['./configuration.component.scss']
})
export class ConfigurationComponent implements OnInit {
  permission: Permission;
  tableActions: CdTableAction[];
  data = [];
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  filters = [
    {
      label: 'Level',
      prop: 'level',
      value: 'basic',
      options: ['basic', 'advanced', 'dev'],
      applyFilter: (row, value) => {
        enum Level {
          basic = 0,
          advanced = 1,
          dev = 2
        }

        const levelVal = Level[value];

        return Level[row.level] <= levelVal;
      }
    },
    {
      label: 'Service',
      prop: 'services',
      value: 'any',
      options: ['any', 'mon', 'mgr', 'osd', 'mds', 'common', 'mds_client', 'rgw'],
      applyFilter: (row, value) => {
        if (value === 'any') {
          return true;
        }

        return row.services.includes(value);
      }
    },
    {
      label: 'Source',
      prop: 'source',
      value: 'any',
      options: ['any', 'mon'],
      applyFilter: (row, value) => {
        if (value === 'any') {
          return true;
        }

        if (!row.hasOwnProperty('source')) {
          return false;
        }

        return row.source.includes(value);
      }
    }
  ];

  @ViewChild('confValTpl')
  public confValTpl: TemplateRef<any>;
  @ViewChild('confFlagTpl')
  public confFlagTpl: TemplateRef<any>;

  constructor(
    private authStorageService: AuthStorageService,
    private configurationService: ConfigurationService
  ) {
    this.permission = this.authStorageService.getPermissions().configOpt;
    const getConfigOptUri = () =>
      this.selection.first() && `${encodeURI(this.selection.first().name)}`;
    const editAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      routerLink: () => `/configuration/edit/${getConfigOptUri()}`,
      name: 'Edit'
    };
    this.tableActions = [editAction];
  }

  ngOnInit() {
    this.columns = [
      { canAutoResize: true, prop: 'name' },
      { prop: 'desc', name: 'Description', cellClass: 'wrap' },
      {
        prop: 'value',
        name: 'Current value',
        cellClass: 'wrap',
        cellTemplate: this.confValTpl
      },
      { prop: 'default', cellClass: 'wrap' }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getConfigurationList(context: CdTableFetchDataContext) {
    this.configurationService.getConfigData().subscribe(
      (data: any) => {
        this.data = data;
      },
      () => {
        context.error();
      }
    );
  }

  updateFilter() {
    this.data = [...this.data];
  }
}
