import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import {
  RbdConfigurationEntry,
  RbdConfigurationSourceField,
  RbdConfigurationType
} from '../../../shared/models/configuration';
import { RbdConfigurationSourcePipe } from '../../../shared/pipes/rbd-configuration-source.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { RbdConfigurationService } from '../../../shared/services/rbd-configuration.service';

@Component({
  selector: 'cd-rbd-configuration-table',
  templateUrl: './rbd-configuration-list.component.html',
  styleUrls: ['./rbd-configuration-list.component.scss']
})
export class RbdConfigurationListComponent implements OnInit, OnChanges {
  @Input()
  data: RbdConfigurationEntry[];
  poolConfigurationColumns: CdTableColumn[];
  @ViewChild('configurationSourceTpl')
  configurationSourceTpl: TemplateRef<any>;
  @ViewChild('configurationValueTpl')
  configurationValueTpl: TemplateRef<any>;
  @ViewChild('poolConfTable')
  poolConfTable: TableComponent;

  readonly sourceField = RbdConfigurationSourceField;
  readonly typeField = RbdConfigurationType;

  constructor(
    public formatterService: FormatterService,
    private rbdConfigurationService: RbdConfigurationService,
    private i18n: I18n
  ) {}

  ngOnInit() {
    this.poolConfigurationColumns = [
      { prop: 'displayName', name: this.i18n('Name') },
      { prop: 'description', name: this.i18n('Description') },
      { prop: 'name', name: this.i18n('Key') },
      {
        prop: 'source',
        name: this.i18n('Source'),
        cellTemplate: this.configurationSourceTpl,
        pipe: new RbdConfigurationSourcePipe()
      },
      { prop: 'value', name: this.i18n('Value'), cellTemplate: this.configurationValueTpl }
    ];
  }

  ngOnChanges(): void {
    if (!this.data) {
      return;
    }
    // Filter settings out which are not listed in RbdConfigurationService
    this.data = this.data.filter((row) =>
      this.rbdConfigurationService
        .getOptionFields()
        .map((o) => o.name)
        .includes(row.name)
    );
  }
}
