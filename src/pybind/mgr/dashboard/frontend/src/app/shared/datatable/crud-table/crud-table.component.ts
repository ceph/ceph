import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { CrudMetadata } from '~/app/shared/models/crud-table-metadata';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { TimerService } from '~/app/shared/services/timer.service';
import { CdTableSelection } from '../../models/cd-table-selection';
import { Permission, Permissions } from '../../models/permissions';
import { AuthStorageService } from '../../services/auth-storage.service';

@Component({
  selector: 'cd-crud-table',
  templateUrl: './crud-table.component.html',
  styleUrls: ['./crud-table.component.scss']
})
export class CRUDTableComponent implements OnInit {
  @ViewChild('badgeDictTpl')
  public badgeDictTpl: TemplateRef<any>;
  @ViewChild('dateTpl')
  public dateTpl: TemplateRef<any>;
  @ViewChild('durationTpl')
  public durationTpl: TemplateRef<any>;

  data$: Observable<any>;
  meta$: Observable<CrudMetadata>;
  meta: CrudMetadata;
  permissions: Permissions;
  permission: Permission;
  selection = new CdTableSelection();
  tabs = {};

  constructor(
    private authStorageService: AuthStorageService,
    private timerService: TimerService,
    private dataGatewayService: DataGatewayService,
    private activatedRoute: ActivatedRoute
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    /* The following should be simplified with a wrapper that
    converts .data to @Input args. For example:
    https://medium.com/@andrewcherepovskiy/passing-route-params-into-angular-components-input-properties-fc85c34c9aca
    */
    this.activatedRoute.data.subscribe((data: any) => {
      const resource: string = data.resource;
      this.tabs = data.tabs;
      this.dataGatewayService
        .list(`ui-${resource}`)
        .subscribe((response: CrudMetadata) => this.processMeta(response));
      this.data$ = this.timerService.get(() => this.dataGatewayService.list(resource));
    });
  }

  processMeta(meta: CrudMetadata) {
    const toCamelCase = (test: string) =>
      test
        .split('-')
        .reduce(
          (res: string, word: string, i: number) =>
            i === 0
              ? word.toLowerCase()
              : `${res}${word.charAt(0).toUpperCase()}${word.substr(1).toLowerCase()}`,
          ''
        );
    this.permission = this.permissions[toCamelCase(meta.permissions[0])];
    const templates = {
      badgeDict: this.badgeDictTpl,
      date: this.dateTpl,
      duration: this.durationTpl
    };
    meta.table.columns.forEach((element, index) => {
      if (element['cellTemplate'] !== undefined) {
        meta.table.columns[index]['cellTemplate'] = templates[element['cellTemplate'] as string];
      }
    });
    // isHidden flag does not work as expected somehow so the best ways to enforce isHidden is
    // to filter the columns manually instead of letting isHidden flag inside table.component to
    // work.
    meta.table.columns = meta.table.columns.filter((col: any) => {
      return !col['isHidden'];
    });
    this.meta = meta;
  }
}
