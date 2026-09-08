import { Component, OnInit } from '@angular/core';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

@Component({
  selector: 'cd-create-cluster-step-4',
  templateUrl: './create-cluster-step-4.component.html',
  styleUrls: ['./create-cluster-step-4.component.scss'],
  standalone: false
})
export class CreateClusterStep4Component implements OnInit, TearsheetStep {
  formGroup: CdFormGroup;

  ngOnInit() {
    this.formGroup = new CdFormGroup({});
  }
}
