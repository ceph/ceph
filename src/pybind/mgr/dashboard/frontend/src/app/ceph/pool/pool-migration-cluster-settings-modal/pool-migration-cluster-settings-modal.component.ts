import { Component, Inject, Optional } from '@angular/core';
import { UntypedFormControl, UntypedFormGroup, Validators } from '@angular/forms';

import { BaseModal } from 'carbon-components-angular';
import { forkJoin, Observable } from 'rxjs';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { OsdService } from '~/app/shared/api/osd.service';
import {
  addPoolMigrationFlag,
  EXPERIMENTAL_FEATURES_CONFIG,
  MIN_POOL_MIGRATION_RELEASE
} from '../pool';

@Component({
  selector: 'cd-pool-migration-cluster-settings-modal',
  templateUrl: './pool-migration-cluster-settings-modal.component.html',
  standalone: false
})
export class PoolMigrationClusterSettingsModalComponent extends BaseModal {
  form: UntypedFormGroup;

  constructor(
    private configurationService: ConfigurationService,
    private osdService: OsdService,
    @Optional() @Inject('needsFlag') public needsFlag: boolean,
    @Optional() @Inject('needsMinCompatClient') public needsMinCompatClient: boolean,
    @Optional() @Inject('flagValue') private flagValue: string,
    @Optional() @Inject('onComplete') private onComplete: () => void
  ) {
    super();
    // Setting the minimum compatible client is irreversible: require explicit acknowledgement.
    this.form = new UntypedFormGroup({
      understand: new UntypedFormControl(
        false,
        this.needsMinCompatClient ? Validators.requiredTrue : []
      )
    });
  }

  onSubmit() {
    const requests: Observable<unknown>[] = [];
    if (this.needsFlag) {
      requests.push(
        this.configurationService.create({
          name: EXPERIMENTAL_FEATURES_CONFIG,
          value: [{ section: 'global', value: addPoolMigrationFlag(this.flagValue) }],
          force_update: false
        })
      );
    }
    if (this.needsMinCompatClient) {
      requests.push(this.osdService.setMinCompatClient(MIN_POOL_MIGRATION_RELEASE));
    }

    forkJoin(requests).subscribe({
      next: () => {
        this.closeModal();
        this.onComplete?.();
      },
      error: () => this.form.setErrors({ cdSubmitButton: true })
    });
  }
}
