import { Component, OnInit, ViewEncapsulation } from '@angular/core';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

@Component({
  selector: 'cd-create-cluster-step-1',
  templateUrl: './create-cluster-step-1.component.html',
  styleUrls: ['./create-cluster-step-1.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CreateClusterStep1Component implements OnInit, TearsheetStep {
  formGroup: CdFormGroup;

  ngOnInit() {
    this.formGroup = new CdFormGroup({});
  }
}
