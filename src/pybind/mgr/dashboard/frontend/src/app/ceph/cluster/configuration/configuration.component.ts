import { Component, OnInit } from '@angular/core';

import { ConfigurationService } from '../../../shared/api/configuration.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-configuration',
  templateUrl: './configuration.component.html',
  styleUrls: ['./configuration.component.scss']
})
export class ConfigurationComponent implements OnInit {
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
    }
  ];

  constructor(private configurationService: ConfigurationService) {}

  ngOnInit() {
    this.columns = [
      { flexGrow: 2, canAutoResize: true, prop: 'name' },
      { flexGrow: 2, prop: 'desc', name: 'Description', cellClass: 'wrap' },
      { flexGrow: 2, prop: 'long_desc', name: 'Long description', cellClass: 'wrap' },
      { flexGrow: 1, prop: 'type' },
      { flexGrow: 1, prop: 'level' },
      { flexGrow: 1, prop: 'default', cellClass: 'wrap' },
      { flexGrow: 2, prop: 'daemon_default', name: 'Daemon default' },
      { flexGrow: 1, prop: 'tags', name: 'Tags' },
      { flexGrow: 1, prop: 'services', name: 'Services' },
      { flexGrow: 1, prop: 'see_also', name: 'See_also', cellClass: 'wrap' },
      { flexGrow: 1, prop: 'max', name: 'Max' },
      { flexGrow: 1, prop: 'min', name: 'Min' }
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
