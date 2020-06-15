import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { ListWithDetails } from '../../../../shared/classes/list-with-details.class';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { Icons } from '../../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permission } from '../../../../shared/models/permissions';
import { CdDatePipe } from '../../../../shared/pipes/cd-date.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { PrometheusAlertService } from '../../../../shared/services/prometheus-alert.service';
import { URLBuilderService } from '../../../../shared/services/url-builder.service';

const BASE_URL = 'silence'; // as only silence actions can be used

@Component({
  selector: 'cd-active-alert-list',
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  templateUrl: './active-alert-list.component.html',
  styleUrls: ['./active-alert-list.component.scss']
})
export class ActiveAlertListComponent extends ListWithDetails implements OnInit {
  @ViewChild('externalLinkTpl', { static: true })
  externalLinkTpl: TemplateRef<any>;
  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  permission: Permission;
  selection = new CdTableSelection();
  icons = Icons;
  customCss = {
    'badge badge-danger': 'active',
    'badge badge-warning': 'unprocessed',
    'badge badge-info': 'suppressed'
  };

  constructor(
    // NotificationsComponent will refresh all alerts every 5s (No need to do it here as well)
    private authStorageService: AuthStorageService,
    public prometheusAlertService: PrometheusAlertService,
    private urlBuilder: URLBuilderService,
    private i18n: I18n,
    private cdDatePipe: CdDatePipe
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().prometheus;
    this.tableActions = [
      {
        permission: 'create',
        canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection || selection.first().cdExecuting,
        icon: Icons.add,
        routerLink: () =>
          '/monitoring' + this.urlBuilder.getCreateFrom(this.selection.first().fingerprint),
        name: this.i18n('Create Silence')
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'labels.alertname',
        flexGrow: 2
      },
      {
        name: this.i18n('Job'),
        prop: 'labels.job',
        flexGrow: 2
      },
      {
        name: this.i18n('Severity'),
        prop: 'labels.severity'
      },
      {
        name: this.i18n('State'),
        prop: 'status.state',
        cellTransformation: CellTemplate.classAdding
      },
      {
        name: this.i18n('Started'),
        prop: 'startsAt',
        pipe: this.cdDatePipe
      },
      {
        name: this.i18n('URL'),
        prop: 'generatorURL',
        sortable: false,
        cellTemplate: this.externalLinkTpl
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
