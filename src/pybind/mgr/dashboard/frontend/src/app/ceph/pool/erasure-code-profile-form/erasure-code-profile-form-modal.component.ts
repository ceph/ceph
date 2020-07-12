import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';

import { ErasureCodeProfileService } from '../../../shared/api/erasure-code-profile.service';
import { CrushNodeSelectionClass } from '../../../shared/classes/crush.node.selection.class';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { CrushNode } from '../../../shared/models/crush-node';
import { ErasureCodeProfile } from '../../../shared/models/erasure-code-profile';
import { FinishedTask } from '../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-erasure-code-profile-form-modal',
  templateUrl: './erasure-code-profile-form-modal.component.html',
  styleUrls: ['./erasure-code-profile-form-modal.component.scss']
})
export class ErasureCodeProfileFormModalComponent extends CrushNodeSelectionClass
  implements OnInit {
  @Output()
  submitAction = new EventEmitter();

  tooltips = this.ecpService.formTooltips;
  PLUGIN = {
    LRC: 'lrc', // Locally Repairable Erasure Code
    SHEC: 'shec', // Shingled Erasure Code
    JERASURE: 'jerasure', // default
    ISA: 'isa' // Intel Storage Acceleration
  };
  plugin = this.PLUGIN.JERASURE;

  form: CdFormGroup;
  plugins: string[];
  names: string[];
  techniques: string[];
  action: string;
  resource: string;
  lrcGroups: number;
  lrcMultiK: number;

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    private taskWrapper: TaskWrapperService,
    private ecpService: ErasureCodeProfileService,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.action = this.actionLabels.CREATE;
    this.resource = this.i18n('EC Profile');
    this.createForm();
    this.setJerasureDefaults();
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
      plugin: [this.PLUGIN.JERASURE, [Validators.required]],
      k: [
        4, // Will be overwritten with plugin defaults
        [
          Validators.required,
          Validators.min(2),
          CdValidators.custom('max', () => this.baseValueValidation(true)),
          CdValidators.custom('unequal', (v: number) => this.lrcDataValidation(v)),
          CdValidators.custom('kLowerM', (v: number) => this.shecDataValidation(v))
        ]
      ],
      m: [
        2, // Will be overwritten with plugin defaults
        [
          Validators.required,
          Validators.min(1),
          CdValidators.custom('max', () => this.baseValueValidation())
        ]
      ],
      crushFailureDomain: '', // Will be preselected
      crushRoot: null, // Will be preselected
      crushDeviceClass: '', // Will be preselected
      directory: '',
      // Only for 'jerasure' and 'isa' use
      technique: 'reed_sol_van',
      // Only for 'jerasure' use
      packetSize: [2048, [Validators.min(1)]],
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
          Validators.min(1),
          CdValidators.custom('cGreaterM', (v: number) => this.shecDurabilityValidation(v))
        ]
      ]
    });
    this.form.get('k').valueChanges.subscribe(() => this.updateValidityOnChange(['m', 'l']));
    this.form.get('m').valueChanges.subscribe(() => this.updateValidityOnChange(['k', 'l', 'c']));
    this.form.get('l').valueChanges.subscribe(() => this.updateValidityOnChange(['k', 'm']));
    this.form.get('plugin').valueChanges.subscribe((plugin) => this.onPluginChange(plugin));
  }

  private baseValueValidation(dataChunk: boolean = false): boolean {
    return this.validValidation(() => {
      return (
        this.getKMSum() > this.deviceCount &&
        this.form.getValue('k') > this.form.getValue('m') === dataChunk
      );
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

  private updateValidityOnChange(names: string[]) {
    names.forEach((name) => this.form.get(name).updateValueAndValidity({ emitEvent: false }));
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
    }
    this.updateValidityOnChange(['m']); // Triggers k, m, c and l
  }

  private setJerasureDefaults() {
    this.setDefaults({
      k: 4,
      m: 2
    });
    this.techniques = [
      'reed_sol_van',
      'reed_sol_r6_op',
      'cauchy_orig',
      'cauchy_good',
      'liberation',
      'blaum_roth',
      'liber8tion'
    ];
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
    this.setDefaults({
      k: 7,
      m: 3
    });
    this.techniques = ['reed_sol_van', 'cauchy'];
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

  private setDefaults(defaults: object) {
    Object.keys(defaults).forEach((controlName) => {
      const control = this.form.get(controlName);
      const value = control.value;
      let overwrite = control.pristine;
      /**
       * As k, m, c and l are now set touched and dirty on the beginning, plugin change will
       * overwrite their values as we can't determine if the user has changed anything.
       * k and m can have two default values where as l and c can only have one,
       * so there is no need to overwrite them.
       */
      if ('k' === controlName) {
        overwrite = [4, 7].includes(value);
      } else if ('m' === controlName) {
        overwrite = [2, 3].includes(value);
      }
      if (overwrite) {
        this.form.get(controlName).setValue(defaults[controlName]);
      }
    });
  }

  ngOnInit() {
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
            this.form.get('crushDeviceClass')
          );
          this.plugins = plugins;
          this.names = names;
          this.form.silentSet('directory', directory);
          this.preValidateNumericInputFields();
        }
      );
  }

  /**
   * This allows k, m, l and c to be validated instantly on change, before the
   * fields got changed before by the user.
   */
  private preValidateNumericInputFields() {
    const kml = ['k', 'm', 'l', 'c'].map((name) => this.form.get(name));
    kml.forEach((control) => {
      control.markAsTouched();
      control.markAsDirty();
    });
    kml[1].updateValueAndValidity(); // Update validity of k, m, c and l
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
          this.activeModal.close();
          this.submitAction.emit(profile);
        }
      });
  }

  private createJson() {
    const pluginControls = {
      technique: [this.PLUGIN.ISA, this.PLUGIN.JERASURE],
      packetSize: [this.PLUGIN.JERASURE],
      l: [this.PLUGIN.LRC],
      crushLocality: [this.PLUGIN.LRC],
      c: [this.PLUGIN.SHEC]
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
      crushRoot: 'crush-root',
      crushDeviceClass: 'crush-device-class',
      packetSize: 'packetsize',
      crushLocality: 'crush-locality'
    };
    const value = this.form.getValue(name);
    ecp[differentApiAttributes[name] || name] = name === 'crushRoot' ? value.name : value;
  }
}
