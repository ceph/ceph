import { Component, OnInit } from '@angular/core';

import _ from 'lodash';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { HostService } from '~/app/shared/api/host.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { InventoryDevice } from '../inventory/inventory-devices/inventory-device.model';

@Component({
  selector: 'cd-create-cluster-review',
  templateUrl: './create-cluster-review.component.html',
  styleUrls: ['./create-cluster-review.component.scss']
})
export class CreateClusterReviewComponent implements OnInit {
  hosts: object[] = [];
  hostsDetails: object;
  hostsByService: object;
  hostsCount: number;
  serviceCount: number;
  serviceOccurrences = {};
  hostsCountPerService: object[] = [];
  uniqueServices: Set<string> = new Set();
  filteredDevices: InventoryDevice[] = [];
  capacity = 0;
  services: Array<CephServiceSpec> = [];

  constructor(
    public wizardStepsService: WizardStepsService,
    public cephServiceService: CephServiceService,
    public hostService: HostService
  ) {}

  ngOnInit() {
    this.hostsDetails = {
      columns: [
        {
          prop: 'hostname',
          name: $localize`Host Name`,
          flexGrow: 2
        },
        {
          name: $localize`Labels`,
          prop: 'labels',
          flexGrow: 1,
          cellTransformation: CellTemplate.badge,
          customTemplateConfig: {
            class: 'badge-dark'
          }
        }
      ]
    };

    this.hostsByService = {
      columns: [
        {
          prop: 'service_type',
          name: $localize`Services`,
          flexGrow: 1,
          cellTransformation: CellTemplate.badge,
          customTemplateConfig: {
            class: 'badge-dark'
          }
        },
        {
          name: $localize`Number of Hosts`,
          prop: 'hosts_per_service',
          flexGrow: 1
        }
      ]
    };

    this.cephServiceService.list().subscribe((resp: Array<CephServiceSpec>) => {
      this.services = resp;
      this.serviceCount = this.services.length;

      _.forEach(this.services, (serviceKey) => {
        this.serviceOccurrences[serviceKey['service_type']] =
          (this.serviceOccurrences[serviceKey['service_type']] || 0) + 1;
        this.uniqueServices.add(serviceKey['service_type']);
      });

      this.uniqueServices.forEach((serviceType) => {
        this.hostsCountPerService.push({
          service_type: serviceType,
          hosts_per_service: this.serviceOccurrences[serviceType]
        });
      });

      this.hostsByService['data'] = [...this.hostsCountPerService];
    });

    this.hostService.list().subscribe((resp: object[]) => {
      this.hosts = resp;
      this.hostsCount = this.hosts.length;
      this.hostsDetails['data'] = [...this.hosts];
    });

    this.filteredDevices = this.wizardStepsService.osdDevices;
    this.capacity = this.wizardStepsService.osdCapacity;
  }
}
