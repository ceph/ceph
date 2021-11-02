import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { CrudMetadata } from '~/app/shared/models/crud-table-metadata';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { TimerService } from '~/app/shared/services/timer.service';

@Component({
  selector: 'cd-crud-table',
  templateUrl: './crud-table.component.html',
  styleUrls: ['./crud-table.component.scss']
})
export class CRUDTableComponent implements OnInit {
  @ViewChild('badgeDictTpl')
  public badgeDictTpl: TemplateRef<any>;

  data$: Observable<any>;
  meta$: Observable<CrudMetadata>;
  meta: CrudMetadata;

  constructor(
    private timerService: TimerService,
    private dataGatewayService: DataGatewayService,
    private activatedRoute: ActivatedRoute
  ) {}

  ngOnInit() {
    /* The following should be simplified with a wrapper that
    converts .data to @Input args. For example:
    https://medium.com/@andrewcherepovskiy/passing-route-params-into-angular-components-input-properties-fc85c34c9aca
    */
    this.activatedRoute.data.subscribe((data) => {
      const resource: string = data.resource;
      this.dataGatewayService
        .list(`ui-${resource}`)
        .subscribe((response) => this.processMeta(response));
      this.data$ = this.timerService.get(() => this.dataGatewayService.list(resource));
    });
  }

  processMeta(meta: CrudMetadata) {
    this.meta = meta;
    const templates = {
      badgeDict: this.badgeDictTpl
    };
    this.meta.table.columns.forEach((element, index) => {
      if (element['cellTemplate'] !== undefined) {
        this.meta.table.columns[index]['cellTemplate'] =
          templates[element['cellTemplate'] as string];
      }
    });
  }
}
