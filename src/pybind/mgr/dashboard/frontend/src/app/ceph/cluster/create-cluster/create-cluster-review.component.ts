import { Component, OnInit, ViewEncapsulation } from '@angular/core';

import _ from 'lodash';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { HostFactsCapacitySource, HostService } from '~/app/shared/api/host.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';

@Component({
  selector: 'cd-create-cluster-review',
  templateUrl: './create-cluster-review.component.html',
  styleUrls: ['./create-cluster-review.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CreateClusterReviewComponent implements OnInit {
  hosts: object[] = [];
  hostsCount: number;
  totalDevices: number;
  totalCapacity = 0;
  services: Array<CephServiceSpec> = [];
  totalCPUs = 0;
  /** Raw bytes; formatted in the template (sum-then-format). */
  totalMemory: number | undefined;
  deploymentDescText: string = '-';
  isSimpleDeployment = true;

  constructor(
    public wizardStepsService: WizardStepsService,
    public cephServiceService: CephServiceService,
    public hostService: HostService,
    private osdService: OsdService,
    private authStorageService: AuthStorageService
  ) {}

  ngOnInit() {
    let dataDevices = 0;
    let dataDeviceCapacity = 0;
    let walDevices = 0;
    let walDeviceCapacity = 0;
    let dbDevices = 0;
    let dbDeviceCapacity = 0;

    this.isSimpleDeployment = this.osdService.isDeployementModeSimple;
    const hostContext = new CdTableFetchDataContext(() => undefined);
    this.hostService.list(hostContext.toParams(), 'true').subscribe((resp: object[]) => {
      this.hosts = resp;
      this.hostsCount = this.hosts.length;
      this.totalCPUs = _.sumBy(this.hosts, (hostKey) => Number(hostKey['cpu_count']) || 0);
      // Authoritative total: sum raw bytes, format once in the template.
      this.totalMemory = this.hostService.getHostsTotalMemoryBytes(
        this.hosts as HostFactsCapacitySource[]
      );
    });

    if (this.osdService.osdDevices['data']) {
      dataDevices = this.osdService.osdDevices['data']?.length;
      dataDeviceCapacity = this.osdService.osdDevices['data']['capacity'];
    }

    if (this.osdService.osdDevices['wal']) {
      walDevices = this.osdService.osdDevices['wal']?.length;
      walDeviceCapacity = this.osdService.osdDevices['wal']['capacity'];
    }

    if (this.osdService.osdDevices['db']) {
      dbDevices = this.osdService.osdDevices['db']?.length;
      dbDeviceCapacity = this.osdService.osdDevices['db']['capacity'];
    }

    if (this.isSimpleDeployment && this.authStorageService.getPermissions().osd?.read) {
      this.osdService.getDeploymentOptions().subscribe((optionsObj) => {
        if (!_.isEmpty(optionsObj)) {
          Object.keys(optionsObj.options).forEach((option) => {
            if (
              this.osdService.selectedFormValues &&
              this.osdService.selectedFormValues.get('deploymentOption').value === option
            ) {
              this.deploymentDescText = optionsObj.options[option].desc;
            }
          });
        }
      });
    }

    this.totalDevices = dataDevices + walDevices + dbDevices;
    this.osdService.osdDevices['totalDevices'] = this.totalDevices;
    this.totalCapacity = dataDeviceCapacity + walDeviceCapacity + dbDeviceCapacity;
  }
}
