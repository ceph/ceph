import { Component, OnInit, ViewChild } from '@angular/core';
import { combineLatest } from 'rxjs';

import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService, NamespaceUpdateRequest } from '~/app/shared/api/nvmeof.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Icons } from '~/app/shared/enum/icons.enum';
import { FormButtonPanelComponent } from '~/app/shared/components/form-button-panel/form-button-panel.component';
import { FormatterService } from '~/app/shared/services/formatter.service';

@Component({
  selector: 'cd-nvmeof-namespace-expand-modal',
  templateUrl: './nvmeof-namespace-expand-modal.component.html',
  styleUrls: ['./nvmeof-namespace-expand-modal.component.scss'],
  standalone: false
})
export class NvmeofNamespaceExpandModalComponent implements OnInit {
  subsystemNQN: string;
  nsid: string;
  group: string;

  nsForm: CdFormGroup;
  currentBytes: number;
  currentSizeGiB: number;
  imageName: string;
  expandText: string = $localize`Expand`;
  icons = Icons;
  INVALID_TEXTS: Record<string, string> = {
    required: $localize`This field is required.`,
    minSize: $localize`Value must be greater than the current image size.`
  };

  minSize: number = 0;

  @ViewChild(FormButtonPanelComponent)
  formButtonPanel: FormButtonPanelComponent;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private nvmeofService: NvmeofService,
    private route: ActivatedRoute,
    private router: Router,
    public taskWrapper: TaskWrapperService,
    private formatter: FormatterService
  ) {}

  ngOnInit() {
    this.createForm();
    combineLatest([this.route.params, this.route.queryParams]).subscribe(
      ([params, queryParams]) => {
        this.subsystemNQN = params['subsystem_nqn'];
        this.nsid = params['nsid'];
        this.group = queryParams['group'];

        if (this.subsystemNQN && this.nsid && this.group) {
          this.initForEdit();
        }
      }
    );
  }

  createForm() {
    this.nsForm = new CdFormGroup({
      image_size: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('minSize', (value: any) => {
            if (this.currentBytes && value !== null && value !== undefined) {
              const bytes = this.formatter.toBytes(`${value}GiB`);
              if (bytes <= this.currentBytes) {
                return { minSize: true };
              }
            }
            return null;
          })
        ]
      })
    });
  }

  initForEdit() {
    this.nvmeofService
      .getNamespace(this.subsystemNQN, this.nsid, this.group)
      .subscribe((res: NvmeofSubsystemNamespace) => {
        this.currentBytes =
          typeof res.rbd_image_size === 'string' ? Number(res.rbd_image_size) : res.rbd_image_size;
        this.imageName = res.rbd_image_name;
        this.currentSizeGiB = this.currentBytes / this.formatter.toBytes('1GiB');
        this.minSize = this.currentSizeGiB;
        this.nsForm.patchValue({
          image_size: this.currentSizeGiB
        });
        this.nsForm.get('image_size').updateValueAndValidity();
      });
  }

  closeModal() {
    this.router.navigate([{ outlets: { modal: null } }], {
      relativeTo: this.route.parent,
      queryParamsHandling: 'preserve'
    });
  }

  onSubmit() {
    if (this.nsForm.invalid) {
      this.nsForm.markAllAsTouched();
      this.nsForm.setErrors({ cdSubmitButton: true });
      if (this.formButtonPanel?.submitButton) {
        this.formButtonPanel.submitButton.loading = false;
      }
      return;
    }

    const image_size = this.nsForm.getValue('image_size');
    const rbdImageSize = this.formatter.toBytes(`${image_size}GiB`);

    const request: NamespaceUpdateRequest = {
      gw_group: this.group,
      rbd_image_size: rbdImageSize
    };

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('nvmeof/namespace/edit', {
          nqn: this.subsystemNQN,
          nsid: this.nsid
        }),
        call: this.nvmeofService.updateNamespace(this.subsystemNQN, this.nsid, request)
      })
      .subscribe({
        complete: () => {
          this.closeModal();
        }
      });
  }
}
