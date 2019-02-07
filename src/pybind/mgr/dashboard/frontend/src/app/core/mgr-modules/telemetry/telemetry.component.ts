import { Component, OnInit } from '@angular/core';
import { Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { MgrModuleService } from '../../../shared/api/mgr-module.service';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';

@Component({
  selector: 'cd-telemetry',
  templateUrl: './telemetry.component.html',
  styleUrls: ['./telemetry.component.scss']
})
export class TelemetryComponent implements OnInit {
  telemetryForm: CdFormGroup;
  error = false;
  loading = false;

  constructor(
    private router: Router,
    private formBuilder: CdFormBuilder,
    private mgrModuleService: MgrModuleService
  ) {
    this.createForm();
  }

  createForm() {
    this.telemetryForm = this.formBuilder.group({
      enabled: [false],
      leaderboard: [false],
      contact: [null, [CdValidators.email]],
      organization: [null, [Validators.maxLength(256)]],
      description: [null, [Validators.maxLength(256)]],
      proxy: [null],
      interval: [72, [Validators.min(24), CdValidators.number(), Validators.required]],
      url: [null]
    });
  }

  ngOnInit() {
    this.loading = true;
    this.mgrModuleService.getConfig('telemetry').subscribe(
      (resp: object) => {
        this.loading = false;
        this.telemetryForm.setValue(resp);
      },
      (error) => {
        this.error = error;
      }
    );
  }

  goToListView() {
    this.router.navigate(['/mgr-modules']);
  }

  onSubmit() {
    // Exit immediately if the form isn't dirty.
    if (this.telemetryForm.pristine) {
      this.goToListView();
    }
    const config = {};
    const fieldNames = [
      'enabled',
      'leaderboard',
      'contact',
      'organization',
      'description',
      'proxy',
      'interval'
    ];
    fieldNames.forEach((fieldName) => {
      config[fieldName] = this.telemetryForm.getValue(fieldName);
    });
    this.mgrModuleService.updateConfig('telemetry', config).subscribe(
      () => {
        this.goToListView();
      },
      () => {
        // Reset the 'Submit' button.
        this.telemetryForm.setErrors({ cdSubmitButton: true });
      }
    );
  }
}
