import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import {
  RbdConfigurationEntry,
  RbdConfigurationSourceField,
  RbdConfigurationType
} from '~/app/shared/models/configuration';
import { RbdConfigurationSourcePipe } from '~/app/shared/pipes/rbd-configuration-source.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { RbdConfigurationService } from '~/app/shared/services/rbd-configuration.service';

@Component({
  selector: 'cd-rbd-configuration-table',
  templateUrl: './rbd-configuration-list.component.html',
  styleUrls: ['./rbd-configuration-list.component.scss']
})
export class RbdConfigurationListComponent implements OnInit, OnChanges {
  @Input()
  data: RbdConfigurationEntry[];
  poolConfigurationColumns: CdTableColumn[];
  @ViewChild('configurationSourceTpl', { static: true })
  configurationSourceTpl: TemplateRef<any>;
  @ViewChild('configurationValueTpl', { static: true })
  configurationValueTpl: TemplateRef<any>;
  @ViewChild('poolConfTable', { static: true })
  poolConfTable: TableComponent;

  readonly sourceField = RbdConfigurationSourceField;
  readonly typeField = RbdConfigurationType;

  constructor(
    public formatterService: FormatterService,
    private rbdConfigurationService: RbdConfigurationService
  ) {}

  ngOnInit() {
    this.poolConfigurationColumns = [
      { prop: 'displayName', name: $localize`Name` },
      { prop: 'description', name: $localize`Description` },
      { prop: 'name', name: $localize`Key` },
      {
        prop: 'source',
        name: $localize`Source`,
        cellTemplate: this.configurationSourceTpl,
        pipe: new RbdConfigurationSourcePipe()
      },
      { prop: 'value', name: $localize`Value`, cellTemplate: this.configurationValueTpl }
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
