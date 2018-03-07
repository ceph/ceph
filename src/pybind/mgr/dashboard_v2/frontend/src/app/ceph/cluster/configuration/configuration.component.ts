import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { ConfigurationService } from '../../../shared/services/configuration.service';

@Component({
  selector: 'cd-configuration',
  templateUrl: './configuration.component.html',
  styleUrls: ['./configuration.component.scss']
})
export class ConfigurationComponent implements OnInit {
  @ViewChild('arrayTmpl') arrayTmpl: TemplateRef<any>;

  data = [];
  columns: any;

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
      options: ['mon', 'mgr', 'osd', 'mds', 'common', 'mds_client', 'rgw', 'any'],
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
      { flexGrow: 2, prop: 'desc', name: 'Description' },
      { flexGrow: 2, prop: 'long_desc', name: 'Long description' },
      { flexGrow: 1, prop: 'type' },
      { flexGrow: 1, prop: 'level' },
      { flexGrow: 1, prop: 'default' },
      { flexGrow: 2, prop: 'daemon_default', name: 'Daemon default' },
      { flexGrow: 1, prop: 'tags', name: 'Tags', cellTemplate: this.arrayTmpl },
      { flexGrow: 1, prop: 'services', name: 'Services', cellTemplate: this.arrayTmpl },
      { flexGrow: 1, prop: 'see_also', name: 'See_also', cellTemplate: this.arrayTmpl },
      { flexGrow: 1, prop: 'max', name: 'Max' },
      { flexGrow: 1, prop: 'min', name: 'Min' }
    ];

    this.fetchData();
  }

  fetchData() {
    this.configurationService.getConfigData().subscribe((data: any) => {
      this.data = data;
    });
  }

  updateFilter() {
    this.data = [...this.data];
  }
}
