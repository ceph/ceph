import { Component, Input, OnChanges, OnInit, ViewChild } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';

import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { SummaryService } from '../../../shared/services/summary.service';
import { Device, InventoryNode } from './inventory.model';

@Component({
  selector: 'cd-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.scss']
})
export class InventoryComponent implements OnChanges, OnInit {
  @ViewChild(TableComponent, { static: false })
  table: TableComponent;

  @Input() hostname = '';

  checkingOrchestrator = true;
  orchestratorExist = false;
  docsUrl: string;

  columns: Array<CdTableColumn> = [];
  devices: Array<Device> = [];
  isLoadingDevices = false;

  constructor(
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private dimlessBinary: DimlessBinaryPipe,
    private i18n: I18n,
    private orchService: OrchestratorService,
    private summaryService: SummaryService
  ) {}

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Device path'),
        prop: 'id',
        flexGrow: 1
      },
      {
        name: this.i18n('Type'),
        prop: 'type',
        flexGrow: 1
      },
      {
        name: this.i18n('Size'),
        prop: 'size',
        flexGrow: 1,
        pipe: this.dimlessBinary
      },
      {
        name: this.i18n('Rotates'),
        prop: 'rotates',
        flexGrow: 1
      },
      {
        name: this.i18n('Available'),
        prop: 'available',
        flexGrow: 1
      },
      {
        name: this.i18n('Model'),
        prop: 'model',
        flexGrow: 1
      }
    ];

    if (!this.hostname) {
      const hostColumn = {
        name: this.i18n('Hostname'),
        prop: 'hostname',
        flexGrow: 1
      };
      this.columns.splice(0, 0, hostColumn);
    }

    // duplicated code with grafana
    const subs = this.summaryService.subscribe((summary: any) => {
      if (!summary) {
        return;
      }

      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/mgr/orchestrator_cli/`;

      setTimeout(() => {
        subs.unsubscribe();
      }, 0);
    });

    this.orchService.status().subscribe((data: { available: boolean }) => {
      this.orchestratorExist = data.available;
      this.checkingOrchestrator = false;
    });
  }

  ngOnChanges() {
    if (this.orchestratorExist) {
      this.devices = [];
      this.table.reloadData();
    }
  }

  getInventory(context: CdTableFetchDataContext) {
    if (this.isLoadingDevices) {
      return;
    }
    this.isLoadingDevices = true;
    this.orchService.inventoryList(this.hostname).subscribe(
      (data: InventoryNode[]) => {
        const devices: Device[] = [];
        data.forEach((node: InventoryNode) => {
          node.devices.forEach((device: Device) => {
            device.hostname = node.name;
            device.uid = `${node.name}-${device.id}`;
            devices.push(device);
          });
        });
        this.devices = devices;
        this.isLoadingDevices = false;
      },
      () => {
        this.isLoadingDevices = false;
        this.devices = [];
        context.error();
      }
    );
  }
}
