import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import {
  UntypedFormControl,
  UntypedFormGroup,
  UntypedFormArray,
  ValidatorFn,
  FormArray,
  Validators,
  AbstractControl,
  ValidationErrors
} from '@angular/forms';
import { ActivatedRoute, Router, Params } from '@angular/router';

import _ from 'lodash';
import { switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { ConfigFormModel } from '~/app/shared/components/config-option/config-option.model';
import { ConfigOptionTypes } from '~/app/shared/components/config-option/config-option.types';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ConfigFormCreateRequestModel } from './configuration-form-create-request.model';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
const RGW = 'rgw';
interface ClientEntityItem {
  content: string;
  name: string;
}

interface CephUser {
  entity: string;
  caps: { [key: string]: string };
  key: string;
}

@Component({
  selector: 'cd-configuration-form',
  templateUrl: './configuration-form.component.html',
  styleUrls: ['./configuration-form.component.scss'],
  standalone: false
})
export class ConfigurationFormComponent extends CdForm implements OnInit {
  configForm!: CdFormGroup;
  response!: ConfigFormModel;
  type!: string;
  inputType!: string;
  humanReadableType!: string;
  minValue!: number;
  maxValue!: number;
  patternHelpText!: string;
  private readonly allSections = ['global', 'mon', 'mgr', 'osd', 'mds'];
  availSections: string[] = [];
  forceUpdate!: boolean;
  clientEntities: CephUser[] = [];
  clientEntityOptions: ClientEntityItem[] = [];

  constructor(
    public actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private router: Router,
    private configService: ConfigurationService,
    private notificationService: NotificationService,
    private modalService: ModalCdsService,
    private dataGatewayService: DataGatewayService,
    private cd: ChangeDetectorRef
  ) {
    super();
    this.createForm();
  }

  get clientEntries(): FormArray {
    return this.configForm.get('clientEntries') as FormArray;
  }

  createForm() {
    const formControls = {
      name: new UntypedFormControl({ value: null }),
      desc: new UntypedFormControl({ value: null }),
      long_desc: new UntypedFormControl({ value: null }),
      values: new UntypedFormGroup({}),
      default: new UntypedFormControl({ value: null }),
      daemon_default: new UntypedFormControl({ value: null }),
      services: new UntypedFormControl([]),
      clientEntries: new UntypedFormArray([])
    };

    this.allSections.forEach((section) => {
      formControls.values.addControl(section, new UntypedFormControl(null));
    });

    this.configForm = new CdFormGroup(formControls);
  }

  ngOnInit() {
    this.route.params
      .pipe(
        switchMap((params: Params) => {
          const configName = params['name'] as string;
          return this.configService.get(configName);
        }),
        switchMap((resp) => {
          this.setResponse(resp as ConfigFormModel);
          return this.dataGatewayService
            .list('api.cluster.user@1.0')
            .pipe(catchError(() => of([])));
        })
      )
      .subscribe({
        next: (users: CephUser[]) => {
          this.clientEntities = users;
          this.prepareClientEntityOptions();
          this.initializeClientEntries();
          this.loadingReady();
        },
        error: () => {
          this.loadingReady();
        }
      });
  }

  getValidators(configOption: ConfigFormModel): ValidatorFn[] | undefined {
    const typeValidators = ConfigOptionTypes.getTypeValidators(configOption);
    if (typeValidators) {
      this.patternHelpText = typeValidators.patternHelpText;

      if ('max' in typeValidators && typeValidators.max !== '') {
        this.maxValue = typeValidators.max;
      }

      if ('min' in typeValidators && typeValidators.min !== '') {
        this.minValue = typeValidators.min;
      }

      return typeValidators.validators;
    }

    return undefined;
  }

  setResponse(response: ConfigFormModel) {
    this.response = response;

    this.availSections = [...this.allSections];

    const validators = this.getValidators(response);
    this.configForm.get('name').setValue(response.name);
    this.configForm.get('desc').setValue(response.desc);
    this.configForm.get('long_desc').setValue(response.long_desc);
    this.configForm.get('default').setValue(response.default);
    this.configForm.get('daemon_default').setValue(response.daemon_default);
    this.configForm.get('services').setValue(response.services);

    if (this.response.value) {
      this.response.value.forEach((value) => {
        if (value.section.startsWith('client')) {
          return;
        }

        // If section is not in standard sections, add it dynamically (e.g., daemon IDs)
        if (!this.availSections.includes(value.section)) {
          this.availSections.push(value.section);
          const valuesGroup = this.configForm.get('values') as UntypedFormGroup;
          valuesGroup.addControl(value.section, new UntypedFormControl(null));
        }
        const sectionValue = this.parseConfigValue(value.value);
        this.configForm.get('values')?.get(value.section)?.setValue(sectionValue);
      });
    }
    this.forceUpdate = !this.response.can_update_at_runtime && response.name.includes(RGW);
    this.availSections.forEach((section) => {
      if (validators) {
        this.configForm.get('values')?.get(section)?.setValidators(validators);
      }
    });

    const currentType = ConfigOptionTypes.getType(response.type);
    this.type = currentType.name;
    this.inputType = currentType.inputType;
    this.humanReadableType = currentType.humanReadable;
  }

  initializeClientEntries() {
    while (this.clientEntries.length) {
      this.clientEntries.removeAt(0);
    }

    const validators = this.getValidators(this.response);
    const allValues = this.response.value || [];

    // Filter for client and client.* values
    const clientSpecificValues = allValues.filter((value) => value.section.startsWith('client'));

    // Tracking unique sections to prevent duplicates
    const uniqueSections = new Set<string>();
    const uniqueClientValues: Array<{ section: string; value: string }> = [];

    clientSpecificValues.forEach((value) => {
      if (!uniqueSections.has(value.section)) {
        uniqueSections.add(value.section);
        uniqueClientValues.push(value);

        // Adding any custom sections that aren't in the dropdown to clientEntityOptions
        const sectionExists = this.clientEntityOptions.some(
          (item) => item.content === value.section
        );
        if (!sectionExists) {
          this.clientEntityOptions.push({
            content: value.section,
            name: value.section
          });
        }
      }
    });

    if (uniqueClientValues.length > 0) {
      uniqueClientValues.forEach((value) => {
        this.addClientEntryWithValue(value.section, value.value, validators);
      });
    }
  }

  private addClientEntryWithValue(section: string, value: string, validators?: ValidatorFn[]) {
    const entryValue = this.parseConfigValue(value);
    this.createClientFormGroup(section, entryValue, validators);
  }

  private parseConfigValue(value: string | boolean | number): string | boolean | number {
    // Handle non-string values
    if (typeof value !== 'string') {
      return value;
    }

    if (this.response.type === 'bool') {
      if (value === 'true') {
        return true;
      }
      if (value === 'false') {
        return false;
      }
    }

    if (['uint', 'int', 'size', 'secs', 'float'].includes(this.response.type)) {
      const trimmedValue = value.trim();
      if (trimmedValue !== '' && !isNaN(Number(trimmedValue))) {
        return Number(trimmedValue);
      }
    }

    return value;
  }

  addClientEntry() {
    const validators = this.getValidators(this.response);
    this.createClientFormGroup('', null, validators);
  }

  private createClientFormGroup(
    clientEntity: string,
    value: string | boolean | number | null,
    validators?: ValidatorFn[]
  ) {
    const valueValidators = validators
      ? [Validators.required, ...validators]
      : [Validators.required];

    const formGroup = new UntypedFormGroup({
      clientEntity: new UntypedFormControl(clientEntity, [
        Validators.required,
        this.clientPrefixValidator(),
        this.clientEntityUniquenessValidator()
      ]),
      value: new UntypedFormControl(value, valueValidators)
    });

    // Revalidate all sibling rows when this row's clientEntity changes
    formGroup.get('clientEntity')?.valueChanges.subscribe(() => {
      this.revalidateClientEntities();
    });

    this.clientEntries.push(formGroup);
  }

  removeClientEntry(index: number) {
    this.clientEntries.removeAt(index);
    // Revalidate remaining entries to update duplicate status
    this.revalidateClientEntities();
    this.cd.detectChanges();
  }

  prepareClientEntityOptions() {
    this.clientEntityOptions = this.clientEntities
      .filter((user) => user.entity.startsWith('client.'))
      .map((user) => ({
        content: user.entity,
        name: user.entity
      }));
  }

  private clientPrefixValidator(): ValidatorFn {
    return (control: AbstractControl): ValidationErrors | null => {
      const value = control.value;
      if (!value) {
        return null;
      }
      if (typeof value === 'string' && !value.startsWith('client.') && value !== 'client') {
        return { clientPrefix: { value: value, requiredPrefix: 'client' } };
      }
      return null;
    };
  }

  private clientEntityUniquenessValidator(): ValidatorFn {
    return (control: AbstractControl): ValidationErrors | null => {
      const value = control.value;
      if (!value) {
        return null;
      }

      const duplicateCount = this.clientEntries.controls.filter((formGroup) => {
        const entityControl = (formGroup as UntypedFormGroup).get('clientEntity');
        return entityControl?.value === value;
      }).length;

      if (duplicateCount > 1) {
        return { clientEntityDuplicate: { value: value } };
      }

      return null;
    };
  }

  private revalidateClientEntities(): void {
    // Revalidate all clientEntity controls to update duplicate status
    this.clientEntries.controls.forEach((formGroup) => {
      const entityControl = (formGroup as UntypedFormGroup).get('clientEntity');
      entityControl?.updateValueAndValidity({ emitEvent: false });
    });
  }

  getStep(type: string, value: any): number | undefined {
    return ConfigOptionTypes.getTypeStep(type, value);
  }

  private hasValue(value: string | boolean | null | undefined): boolean {
    return value !== null && value !== undefined && value !== '';
  }

  createRequest(): ConfigFormCreateRequestModel | null {
    const values: Array<{ section: string; value: string | boolean }> = [];

    // Handle standard sections
    this.availSections.forEach((section) => {
      const sectionValue = this.configForm.get('values')?.get(section)?.value;
      const hadValue = this.response?.value?.some((v) => v.section === section);

      if (this.hasValue(sectionValue)) {
        values.push({ section: section, value: sectionValue });
      } else if (hadValue) {
        values.push({ section: section, value: '' });
      }
    });

    this.clientEntries.controls.forEach((control: AbstractControl) => {
      const formGroup = control as UntypedFormGroup;
      const clientEntity = formGroup.get('clientEntity')?.value as string;
      const value = formGroup.get('value')?.value;

      if (clientEntity && this.hasValue(value)) {
        values.push({ section: clientEntity, value: value });
      }
    });

    // Remove values from client.* that were previously set but are now removed
    const currentClientSections = this.clientEntries.controls
      .map(
        (control: AbstractControl) =>
          (control as UntypedFormGroup).get('clientEntity')?.value as string
      )
      .filter((clientEntity: string | undefined): clientEntity is string => !!clientEntity);

    this.response?.value?.forEach((value) => {
      if (value.section.startsWith('client') && !currentClientSections.includes(value.section)) {
        values.push({ section: value.section, value: '' });
      }
    });

    if (!_.isEqual(this.response.value, values)) {
      const request = new ConfigFormCreateRequestModel();
      request.name = this.configForm.getValue('name');
      request.value = values;
      if (this.forceUpdate) {
        request.force_update = this.forceUpdate;
      }
      return request;
    }

    return null;
  }

  openCriticalConfirmModal() {
    this.modalService.show(DeleteConfirmationModalComponent, {
      buttonText: $localize`Force Edit`,
      actionDescription: $localize`force edit`,
      itemDescription: $localize`configuration`,
      infoMessage: 'Updating this configuration might require restarting the client',
      submitAction: () => {
        this.modalService.dismissAll();
        this.submit();
      }
    });
  }

  submit() {
    const request = this.createRequest();

    if (request) {
      this.configService.create(request).subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Updated config option ${request.name}`
          );
          this.router.navigate(['/configuration']);
        },
        error: () => {
          this.configForm.setErrors({ cdSubmitButton: true });
        }
      });
    } else {
      this.router.navigate(['/configuration']);
    }
  }
}
