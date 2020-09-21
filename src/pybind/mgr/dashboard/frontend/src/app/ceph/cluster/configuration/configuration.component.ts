import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { ConfigurationService } from '../../../shared/api/configuration.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { Icons } from '../../../shared/enum/icons.enum';
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
export class ConfigurationComponent extends ListWithDetails implements OnInit {
  permission: Permission;
  tableActions: CdTableAction[];
  data: any[] = [];
  icons = Icons;
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  filters: CdTableColumn[] = [
    {
      name: $localize`Level`,
      prop: 'level',
      filterOptions: ['basic', 'advanced', 'dev'],
      filterInitValue: 'basic',
      filterPredicate: (row, value) => {
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
      name: $localize`Service`,
      prop: 'services',
      filterOptions: ['mon', 'mgr', 'osd', 'mds', 'common', 'mds_client', 'rgw'],
      filterPredicate: (row, value) => {
        return row.services.includes(value);
      }
    },
    {
      name: $localize`Source`,
      prop: 'source',
      filterOptions: ['mon'],
      filterPredicate: (row, value) => {
        if (!row.hasOwnProperty('source')) {
          return false;
        }
        return row.source.includes(value);
      }
    },
    {
      name: $localize`Modified`,
      prop: 'modified',
      filterOptions: ['yes', 'no'],
      filterPredicate: (row, value) => {
        if (value === 'yes' && row.hasOwnProperty('value')) {
          return true;
        }

        if (value === 'no' && !row.hasOwnProperty('value')) {
          return true;
        }

        return false;
      }
    }
  ];

  @ViewChild('confValTpl', { static: true })
  public confValTpl: TemplateRef<any>;
  @ViewChild('confFlagTpl')
  public confFlagTpl: TemplateRef<any>;

  constructor(
    private authStorageService: AuthStorageService,
    private configurationService: ConfigurationService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().configOpt;
    const getConfigOptUri = () =>
      this.selection.first() && `${encodeURIComponent(this.selection.first().name)}`;
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => `/configuration/edit/${getConfigOptUri()}`,
      name: this.actionLabels.EDIT,
      disable: () => !this.isEditable(this.selection)
    };
    this.tableActions = [editAction];
  }

  ngOnInit() {
    this.columns = [
      { canAutoResize: true, prop: 'name', name: $localize`Name` },
      { prop: 'desc', name: $localize`Description`, cellClass: 'wrap' },
      {
        prop: 'value',
        name: $localize`Current value`,
        cellClass: 'wrap',
        cellTemplate: this.confValTpl
      },
      { prop: 'default', name: $localize`Default`, cellClass: 'wrap' },
      {
        prop: 'can_update_at_runtime',
        name: $localize`Editable`,
        cellTransformation: CellTemplate.checkIcon,
        flexGrow: 0.4,
        cellClass: 'text-center'
      }
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

  isEditable(selection: CdTableSelection): boolean {
    if (selection.selected.length !== 1) {
      return false;
    }

    return selection.selected[0].can_update_at_runtime;
  }
}
