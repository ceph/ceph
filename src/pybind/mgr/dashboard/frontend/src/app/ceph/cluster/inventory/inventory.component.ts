import { Component, Input, OnChanges, OnInit } from '@angular/core';

import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { Icons } from '../../../shared/enum/icons.enum';
import { InventoryDevice } from './inventory-devices/inventory-device.model';

@Component({
  selector: 'cd-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.scss']
})
export class InventoryComponent implements OnChanges, OnInit {
  // Display inventory page only for this hostname, ignore to display all.
  @Input() hostname?: string;

  icons = Icons;

  hasOrchestrator = false;
  docsUrl: string;

  devices: Array<InventoryDevice> = [];

  constructor(private orchService: OrchestratorService) {}

  ngOnInit() {
    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      if (status.available) {
        this.getInventory();
      }
    });
  }

  ngOnChanges() {
    if (this.hasOrchestrator) {
      this.devices = [];
      this.getInventory();
    }
  }

  getInventory() {
    if (this.hostname === '') {
      return;
    }
    this.orchService.inventoryDeviceList(this.hostname).subscribe(
      (devices: InventoryDevice[]) => {
        this.devices = devices;
      },
      () => {
        this.devices = [];
      }
    );
  }

  refresh() {
    this.getInventory();
  }
}
