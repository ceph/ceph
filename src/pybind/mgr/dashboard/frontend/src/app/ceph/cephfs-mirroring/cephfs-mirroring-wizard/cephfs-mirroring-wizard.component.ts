import { Component, OnInit } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { STEP_TITLES_MIRRORING_CONFIGURED } from './cephfs-mirroring-wizard-step.enum';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-cephfs-mirroring-wizard',
  templateUrl: './cephfs-mirroring-wizard.component.html',
  styleUrls: ['./cephfs-mirroring-wizard.component.scss']
})
export class CephfsMirroringWizardComponent implements OnInit {
  stepTitles: Step[] = STEP_TITLES_MIRRORING_CONFIGURED.map((title) => ({
    label: title
  }));
  selectedRole: string = 'source';
  icons = Icons;
  sourceList: string[] = [
    'Sends data to remote clusters',
    'Requires bootstrap token from target',
    'Manages snapshot schedules'
  ];

  targetList: string[] = [
    'Receives data from source clusters',
    'Generates bootstrap token',
    'Stores replicated snapshots'
  ];

  ngOnInit(): void {}

  selectRole(role: string) {
    this.selectedRole = role;
  }
}
