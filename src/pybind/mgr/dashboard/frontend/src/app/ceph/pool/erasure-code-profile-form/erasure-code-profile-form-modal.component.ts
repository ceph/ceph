import {
  ChangeDetectorRef,
  Component,
  EventEmitter,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';
import { AbstractControl, FormGroupDirective, ValidatorFn, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { CrushNodeSelectionClass } from '~/app/shared/classes/crush.node.selection.class';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { CrushNode } from '~/app/shared/models/crush-node';
import { ErasureCodeProfile, CrushFailureDomains } from '~/app/shared/models/erasure-code-profile';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-erasure-code-profile-form-modal',
  templateUrl: './erasure-code-profile-form-modal.component.html',
  styleUrls: ['./erasure-code-profile-form-modal.component.scss'],
  standalone: false
})
export class ErasureCodeProfileFormModalComponent
  extends CrushNodeSelectionClass
  implements OnInit {
  @ViewChild(FormGroupDirective)
  formDir: FormGroupDirective;

  @Output()
  submitAction = new EventEmitter();

  tooltips!: Record<string, any>;
  PLUGIN = {
    LRC: 'lrc', // Locally Repairable Erasure Code
    SHEC: 'shec', // Shingled Erasure Code
    CLAY: 'clay', // Coupled LAYer
    JERASURE: 'jerasure',
    ISA: 'isa' // Intel Storage Acceleration - default
  };
  plugin = this.PLUGIN.ISA;
  icons = Icons;

  form: CdFormGroup;
  plugins: string[];
  names: string[];
  techniques: string[];
  action: string;
  resource: string;
  dCalc: boolean;
  lrcGroups: number;
  lrcMultiK: number;
  selectedCrushRoot: CrushNode;

  public CrushFailureDomains = CrushFailureDomains;

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    private taskWrapper: TaskWrapperService,
    private ecpService: ErasureCodeProfileService,
    public actionLabels: ActionLabelsI18n,
    private cdr: ChangeDetectorRef
  ) {
    super();
    this.action = this.actionLabels.CREATE;
    this.resource = $localize`EC Profile`;
    this.createForm();
    this.setIsaDefaults();
  }

  createForm() {
    this.form = this.formBuilder.group({
      name: [
        null,
        [
          Validators.required,
          Validators.pattern('[A-Za-z0-9_-]+'),
          CdValidators.custom(
            'uniqueName',
            (value: string) => this.names && this.names.indexOf(value) !== -1
          )
        ]
      ],
      plugin: [this.PLUGIN.ISA, [Validators.required]],
      k: [
        7, // Will be overwritten with plugin defaults
        [
          Validators.required,
          Validators.min(2),
          CdValidators.custom('max', () => this.baseValueValidation(true)),
          CdValidators.custom('unequal', (v: number) => this.lrcDataValidation(v)),
          CdValidators.custom('kLowerM', (v: number) => this.shecDataValidation(v))
        ]
      ],
      m: [
        3, // Will be overwritten with plugin defaults
        [
          Validators.required,
          Validators.min(1),
          CdValidators.custom('max', () => this.baseValueValidation())
        ]
      ],
      crushFailureDomain: CrushFailureDomains.Host, // Will be preselected
      crushNumFailureDomains: [
        0,
        [
          CdValidators.requiredIf({ crushOsdsPerFailureDomain: { op: 'minValue', arg1: 1 } }),
          CdValidators.number(false),
          this.crushNumFailureDomainsValidator()
        ]
      ],
      crushOsdsPerFailureDomain: [
        0,
        [
          CdValidators.requiredIf({ crushNumFailureDomains: { op: 'minValue', arg1: 1 } }),
          CdValidators.number(false)
        ]
      ],
      crushRoot: null, // Will be preselected
      crushDeviceClass: '', // Will be preselected
      directory: '',
      // Only for 'jerasure', 'clay' and 'isa' use
      technique: 'reed_sol_van',
      // Only for 'jerasure' use
      packetSize: [2048],
      // Only for 'lrc' use
      l: [
        3, // Will be overwritten with plugin defaults
        [
          Validators.required,
          Validators.min(1),
          CdValidators.custom('unequal', (v: number) => this.lrcLocalityValidation(v))
        ]
      ],
      crushLocality: '', // set to none at the end (same list as for failure domains)
      // Only for 'shec' use
      c: [
        2, // Will be overwritten with plugin defaults
        [
          Validators.required,
          CdValidators.custom('cGreaterM', (v: number) => this.shecDurabilityValidation(v))
        ]
      ],
      // Only for 'clay' use
      d: [
        5, // Will be overwritten with plugin defaults (k+m-1) = k+1 <= d <= k+m-1
        [
          Validators.required,
          CdValidators.custom('dMin', (v: number) => this.dMinValidation(v)),
          CdValidators.custom('dMax', (v: number) => this.dMaxValidation(v))
        ]
      ],
      scalar_mds: [this.PLUGIN.ISA, [Validators.required]] // jerasure or isa or shec
    });
    this.toggleDCalc();
    this.form.get('k').valueChanges.subscribe(() => this.updateValidityOnChange(['m', 'l', 'd']));
    this.form
      .get('m')
      .valueChanges.subscribe(() => this.updateValidityOnChange(['k', 'l', 'c', 'd']));
    this.form.get('l').valueChanges.subscribe(() => {
      this.updateValidityOnChange(['k', 'm']);
      this.form.get('l').updateValueAndValidity({ emitEvent: false });
    });
    this.form.get('plugin').valueChanges.subscribe((plugin) => this.onPluginChange(plugin));
    this.form.get('scalar_mds').valueChanges.subscribe(() => this.setClayDefaultsForScalar());
    this.form.get('crushFailureDomain').valueChanges.subscribe(() => {
      this.form.get('crushNumFailureDomains').updateValueAndValidity();
      this.form.get('crushOsdsPerFailureDomain').updateValueAndValidity();
    });
    this.form.get('crushNumFailureDomains').valueChanges.subscribe(() => {
      this.form.get('k').updateValueAndValidity();
      this.form.get('m').updateValueAndValidity();
    });
    this.form.get('crushOsdsPerFailureDomain').valueChanges.subscribe(() => {
      this.form.get('k').updateValueAndValidity();
      this.form.get('m').updateValueAndValidity();
    });
  }

  private baseValueValidation(dataChunk: boolean = false): boolean {
    return this.validValidation(() => {
      const crushnumfailuredomain = this.form.get('crushNumFailureDomains').value;
      const crushosdfailuredomain = this.form.get('crushOsdsPerFailureDomain').value;
      if (crushnumfailuredomain > 0 || crushosdfailuredomain > 0) {
        return false;
      } else {
        const kMSum =
          this.form.get('crushFailureDomain').value === CrushFailureDomains.Host
            ? this.getKMSum() + 1
            : this.getKMSum();
        return (
          kMSum > this.deviceCount &&
          this.form.getValue('k') > this.form.getValue('m') === dataChunk
        );
      }
    });
  }

  private validValidation(fn: () => boolean, plugin?: string): boolean {
    if (!this.form || plugin ? this.plugin !== plugin : false) {
      return false;
    }
    return fn();
  }

  private getKMSum(): number {
    return this.form.getValue('k') + this.form.getValue('m');
  }

  private lrcDataValidation(k: number): boolean {
    return this.validValidation(() => {
      const m = this.form.getValue('m');
      const l = this.form.getValue('l');
      const km = k + m;
      this.lrcMultiK = k / (km / l);
      return k % (km / l) !== 0;
    }, 'lrc');
  }

  private shecDataValidation(k: number): boolean {
    return this.validValidation(() => {
      const m = this.form.getValue('m');
      return m > k;
    }, 'shec');
  }

  private lrcLocalityValidation(l: number) {
    return this.validValidation(() => {
      const value = this.getKMSum();
      this.lrcGroups = l > 0 ? value / l : 0;
      return l > 0 && value % l !== 0;
    }, 'lrc');
  }

  private shecDurabilityValidation(c: number): boolean {
    return this.validValidation(() => {
      const m = this.form.getValue('m');
      return c > m;
    }, 'shec');
  }

  /*
  Following function is written to implement MSR EC profile validation
  1. When 'Crush num failure domain' >= 1  or 'Crush osds per failue domain' >= 1, it is MSR EC Profile
  2. k+m+1 rule does not applies to MSR EC Profiles
  3. 'Crush num failure domain' <= 'Crush failure domain' (host)
  The function validates 3rd condition
  */
  private crushNumFailureDomainsValidator(): ValidatorFn {
    return (control: AbstractControl): { [key: string]: any } | null => {
      const v = control.value;
      if (!v || v === 0) {
        return null;
      }

      if (!control.parent) {
        return null;
      }

      const crushFailureDomainControl = control.parent.get('crushFailureDomain');
      if (!crushFailureDomainControl) {
        return null; // No validation if crushFailureDomain control doesn't exist
      }

      const crushFailureDomain = crushFailureDomainControl.value;

      // Validate that we have a selected failure domain and it exists in failureDomains
      if (!crushFailureDomain || !this.failureDomains || !this.failureDomains[crushFailureDomain]) {
        return null; // No validation if failure domain is not selected or failureDomains not initialized
      }

      // Get the count for the currently selected failure domain (dynamically based on user selection)
      const availableCount = this.failureDomains[crushFailureDomain].length;
      if (v > availableCount) {
        return { maxFailureDomains: true };
      }
      return null;
    };
  }

  private dMinValidation(d: number): boolean {
    return this.validValidation(() => this.getDMin() > d, 'clay');
  }

  getDMin(): number {
    return this.form.getValue('k') + 1;
  }

  private dMaxValidation(d: number): boolean {
    return this.validValidation(() => d > this.getDMax(), 'clay');
  }

  getDMax(): number {
    const m = this.form.getValue('m');
    const k = this.form.getValue('k');
    return k + m - 1;
  }

  toggleDCalc() {
    this.dCalc = !this.dCalc;
    this.form.get('d')[this.dCalc ? 'disable' : 'enable']();
    this.calculateD();
  }

  private calculateD() {
    if (this.plugin !== this.PLUGIN.CLAY || !this.dCalc) {
      return;
    }
    this.form.silentSet('d', this.getDMax());
  }

  private updateValidityOnChange(names: string[]) {
    names.forEach((name) => {
      if (name === 'd') {
        this.calculateD();
      }
      this.form.get(name).updateValueAndValidity({ emitEvent: false });
    });
  }

  private onPluginChange(plugin: string) {
    this.plugin = plugin;
    if (plugin === this.PLUGIN.JERASURE) {
      this.setJerasureDefaults();
    } else if (plugin === this.PLUGIN.LRC) {
      this.setLrcDefaults();
    } else if (plugin === this.PLUGIN.ISA) {
      this.setIsaDefaults();
    } else if (plugin === this.PLUGIN.SHEC) {
      this.setShecDefaults();
    } else if (plugin === this.PLUGIN.CLAY) {
      this.setClayDefaults();
    }
    this.updateValidityOnChange(['m']); // Triggers k, m, c, d and l
  }

  private setJerasureDefaults() {
    this.techniques = [
      'reed_sol_van',
      'reed_sol_r6_op',
      'cauchy_orig',
      'cauchy_good',
      'liberation',
      'blaum_roth',
      'liber8tion'
    ];
    this.setDefaults({
      k: 4,
      m: 2,
      technique: 'reed_sol_van'
    });
  }

  private setLrcDefaults() {
    this.setDefaults({
      k: 4,
      m: 2,
      l: 3
    });
  }

  private setIsaDefaults() {
    /**
     * Actually k and m are not required - but they will be set to the default values in case
     * if they are not set, therefore it's fine to mark them as required in order to get
     * strange values that weren't set.
     */
    this.techniques = ['reed_sol_van', 'cauchy'];
    this.setDefaults({
      k: 7,
      m: 3,
      technique: 'reed_sol_van'
    });
  }

  private setShecDefaults() {
    /**
     * Actually k, c and m are not required - but they will be set to the default values in case
     * if they are not set, therefore it's fine to mark them as required in order to get
     * strange values that weren't set.
     */
    this.setDefaults({
      k: 4,
      m: 3,
      c: 2
    });
  }

  private setClayDefaults() {
    /**
     * Actually d and scalar_mds are not required - but they will be set to show the default values
     * in case if they are not set, therefore it's fine to mark them as required in order to not get
     * strange values that weren't set.
     *
     * As d would be set to the value k+m-1 for the greatest savings, the form will
     * automatically update d if the automatic calculation is activated (default).
     */
    this.setDefaults({
      k: 4,
      m: 2,
      // d: 5, <- Will be automatically update to 5
      scalar_mds: this.PLUGIN.JERASURE
    });
    this.setClayDefaultsForScalar();
  }

  private setClayDefaultsForScalar() {
    const plugin = this.form.getValue('scalar_mds');
    let defaultTechnique = 'reed_sol_van';
    if (plugin === this.PLUGIN.JERASURE) {
      this.techniques = [
        'reed_sol_van',
        'reed_sol_r6_op',
        'cauchy_orig',
        'cauchy_good',
        'liber8tion'
      ];
    } else if (plugin === this.PLUGIN.ISA) {
      this.techniques = ['reed_sol_van', 'cauchy'];
    } else {
      // this.PLUGIN.SHEC
      defaultTechnique = 'single';
      this.techniques = ['single', 'multiple'];
    }
    this.setDefaults({ technique: defaultTechnique });
  }

  private setDefaults(defaults: object) {
    Object.keys(defaults).forEach((controlName) => {
      const control = this.form.get(controlName);
      const value = control.value;
      /**
       * As k, m, c and l are now set touched and dirty on the beginning, plugin change will
       * overwrite their values as we can't determine if the user has changed anything.
       * k and m can have two default values where as l and c can only have one,
       * so there is no need to overwrite them.
       */
      const overwrite =
        control.pristine ||
        (controlName === 'technique' && !this.techniques.includes(value)) ||
        (controlName === 'k' && [4, 7].includes(value)) ||
        (controlName === 'm' && [2, 3].includes(value));
      if (overwrite) {
        control.setValue(defaults[controlName]); // also validates new value
      } else {
        control.updateValueAndValidity();
      }
    });
  }

  ngOnInit() {
    this.tooltips = this.ecpService.formTooltips;

    this.ecpService
      .getInfo()
      .subscribe(
        ({
          plugins,
          names,
          directory,
          nodes
        }: {
          plugins: string[];
          names: string[];
          directory: string;
          nodes: CrushNode[];
        }) => {
          this.initCrushNodeSelection(
            nodes,
            this.form.get('crushRoot'),
            this.form.get('crushFailureDomain'),
            this.form.get('crushDeviceClass'),
            false
          );
          this.plugins = plugins;
          this.names = names;
          this.form.silentSet('directory', directory);
          this.preValidateNumericInputFields();

          setTimeout(() => {
            const selectElement = document.getElementById('crushRoot') as any;
            if (selectElement) {
              selectElement.value = this.form.get('crushRoot').value;
            }
            this.cdr.detectChanges();
          }, 0);

          if (this.plugins.includes(this.PLUGIN.ISA)) {
            this.setIsaDefaults();
          } else if (this.plugins.includes(this.PLUGIN.JERASURE)) {
            this.setJerasureDefaults();
          }
        }
      );
  }

  /**
   * This allows k, m, l and c to be validated instantly on change, before the
   * fields got changed before by the user.
   */
  private preValidateNumericInputFields() {
    const kml = ['k', 'm', 'l', 'c', 'd'].map((name) => this.form.get(name));
    kml.forEach((control) => {
      control.markAsTouched();
      control.markAsDirty();
    });
    kml[1].updateValueAndValidity(); // Update validity of k, m, c, d and l
  }

  onSubmit() {
    if (this.form.invalid) {
      this.form.setErrors({ cdSubmitButton: true });
      return;
    }
    const profile = this.createJson();
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('ecp/create', { name: profile.name }),
        call: this.ecpService.create(profile)
      })
      .subscribe({
        error: () => {
          this.form.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.closeModal();
          this.submitAction.emit(profile);
        }
      });
  }

  private createJson() {
    const pluginControls = {
      technique: [this.PLUGIN.ISA, this.PLUGIN.JERASURE, this.PLUGIN.CLAY],
      packetSize: [this.PLUGIN.JERASURE],
      l: [this.PLUGIN.LRC],
      crushLocality: [this.PLUGIN.LRC],
      c: [this.PLUGIN.SHEC],
      d: [this.PLUGIN.CLAY],
      scalar_mds: [this.PLUGIN.CLAY]
    };
    const ecp = new ErasureCodeProfile();
    const plugin = this.form.getValue('plugin');
    Object.keys(this.form.controls)
      .filter((name) => {
        const pluginControl = pluginControls[name];
        const value = this.form.getValue(name);
        const usable = (pluginControl && pluginControl.includes(plugin)) || !pluginControl;
        return usable && value && value !== '';
      })
      .forEach((name) => {
        this.extendJson(name, ecp);
      });
    return ecp;
  }

  private extendJson(name: string, ecp: ErasureCodeProfile) {
    const differentApiAttributes = {
      crushFailureDomain: 'crush-failure-domain',
      crushNumFailureDomains: 'crush-num-failure-domains',
      crushOsdsPerFailureDomain: 'crush-osds-per-failure-domain',
      crushRoot: 'crush-root',
      crushDeviceClass: 'crush-device-class',
      packetSize: 'packetsize',
      crushLocality: 'crush-locality'
    };
    const value = this.form.getValue(name);
    ecp[differentApiAttributes[name] || name] = name === 'crushRoot' ? value.name : value;
  }

  onCrushFailureDomainChane() {
    this.form.get('k').updateValueAndValidity();
    this.form.get('m').updateValueAndValidity();
    this.form.get('crushNumFailureDomains').updateValueAndValidity();
    this.form.get('crushOsdsPerFailureDomain').updateValueAndValidity();
  }
}
