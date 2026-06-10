import { Component, EventEmitter, OnInit, Output, ViewEncapsulation } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { DriveGroup } from '../../osd/osd-form/drive-group.model';

@Component({
  selector: 'cd-create-cluster-step-2',
  templateUrl: './create-cluster-step-2.component.html',
  styleUrls: ['./create-cluster-step-2.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CreateClusterStep2Component implements OnInit, TearsheetStep {
  @Output() skipStep = new EventEmitter<void>();

  formGroup: CdFormGroup;
  showForm = false;

  ngOnInit() {
    this.formGroup = new CdFormGroup({
      skipped: new UntypedFormControl(false),
      driveGroup: new UntypedFormControl(null),
      selectedOption: new UntypedFormControl(null),
      simpleDeployment: new UntypedFormControl(true)
    });
  }

  onSkip() {
    this.formGroup.patchValue({ skipped: true });
    this.skipStep.emit();
  }

  onOsdCreated() {
    this.showForm = false;
    this.formGroup.patchValue({ skipped: true });
  }

  onCreateAction() {
    this.showForm = true;
  }

  setDriveGroup(driveGroup: DriveGroup) {
    this.formGroup.patchValue({ driveGroup });
  }

  setDeploymentOptions(option: object) {
    this.formGroup.patchValue({ selectedOption: option });
  }

  setDeploymentMode(mode: boolean) {
    this.formGroup.patchValue({ simpleDeployment: mode });
  }
}
