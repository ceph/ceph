import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { CdDatePipe } from '../../../../shared/pipes/cd-date.pipe';
import { PrometheusAlertService } from '../../../../shared/services/prometheus-alert.service';

@Component({
  selector: 'cd-prometheus-list',
  templateUrl: './prometheus-list.component.html',
  styleUrls: ['./prometheus-list.component.scss']
})
export class PrometheusListComponent implements OnInit {
  @ViewChild('externalLinkTpl')
  externalLinkTpl: TemplateRef<any>;
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  customCss = {
    'label label-danger': 'active',
    'label label-warning': 'unprocessed',
    'label label-info': 'suppressed'
  };

  constructor(
    // NotificationsComponent will refresh all alerts every 5s (No need to do it here as well)
    public prometheusAlertService: PrometheusAlertService,
    private i18n: I18n,
    private cdDatePipe: CdDatePipe
  ) {}

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
