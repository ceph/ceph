import {
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Inject,
  OnInit,
  Optional,
  Output
} from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import _ from 'lodash';
import { Observable } from 'rxjs';
import { debounceTime, distinctUntilChanged, map } from 'rxjs/operators';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import {
  AlertmanagerSilenceMatcher,
  AlertmanagerSilenceMatcherMatch
} from '~/app/shared/models/alertmanager-silence';
import { PrometheusRule } from '~/app/shared/models/prometheus-alerts';
import { PrometheusSilenceMatcherService } from '~/app/shared/services/prometheus-silence-matcher.service';

@Component({
  selector: 'cd-silence-matcher-modal',
  templateUrl: './silence-matcher-modal.component.html',
  styleUrls: ['./silence-matcher-modal.component.scss'],
  standalone: false
})
export class SilenceMatcherModalComponent extends CdForm implements OnInit {
  rules: PrometheusRule[] = [];
  prefillMatcher?: AlertmanagerSilenceMatcher;
  editMode = false;
  @Output() submitAction = new EventEmitter<AlertmanagerSilenceMatcher>();

  form: CdFormGroup;
  nameAttributes = ['alertname', 'instance', 'job', 'severity'];
  possibleValues: string[] = [];
  allPossibleValues: { content: string; value: string }[] = [];
  matcherMatch: AlertmanagerSilenceMatcherMatch = undefined;
  search = (text$: Observable<string>) =>
    text$.pipe(
      debounceTime(200),
      distinctUntilChanged(),
      map((term) =>
        (term === ''
          ? this.possibleValues
          : this.possibleValues.filter((v) => v.toLowerCase().includes(term.toLowerCase()))
        ).slice(0, 10)
      )
    );

  constructor(
    private formBuilder: CdFormBuilder,
    private silenceMatcher: PrometheusSilenceMatcherService,
    public actionLabels: ActionLabelsI18n,
    private cdRef: ChangeDetectorRef,
    @Optional() @Inject('rules') public injectedRules: PrometheusRule[],
    @Optional() @Inject('editMode') public injectedEditMode = false,
    @Optional() @Inject('prefillMatcher') public injectedPrefillMatcher?: AlertmanagerSilenceMatcher
  ) {
    super();
  }

  ngOnInit(): void {
    this.rules = this.injectedRules || [];
    this.editMode = this.injectedEditMode;
    this.prefillMatcher = this.injectedPrefillMatcher || this.prefillMatcher;
    this.createForm();
    if (this.editMode && this.prefillMatcher) {
      this.preFillControls(this.prefillMatcher);
    }
    this.subscribeToChanges();
  }

  private createForm() {
    this.form = this.formBuilder.group({
      name: [null, [Validators.required]],
      value: [{ value: '', disabled: true }, [Validators.required]],
      isRegex: new UntypedFormControl(false)
    });
  }

  private subscribeToChanges() {
    const nameControl = this.form.get('name');
    const valueControl = this.form.get('value');

    if (!nameControl.value) {
      valueControl.disable({ emitEvent: false });
    }

    nameControl.valueChanges.subscribe((name: string | null) => {
      if (!name) {
        valueControl.disable({ emitEvent: false });
      } else {
        valueControl.enable({ emitEvent: false });
        this.setPossibleValues(name);
      }

      this.cdRef.detectChanges();
    });

    valueControl.valueChanges.subscribe((value: string) => {
      const name = nameControl.value;
      if (!name || value === null || value === undefined) {
        return;
      }

      const currentValues = { ...this.form.value, name, value };
      this.matcherMatch = this.silenceMatcher.singleMatch(currentValues, this.rules);
    });
  }

  setPossibleValues(name: string, filterValue: string = ''): void {
    if (!this.rules || !Array.isArray(this.rules)) {
      this.possibleValues = [];
      return;
    }

    const values = this.rules
      .map((r) => _.get(r, this.silenceMatcher.getAttributePath(name)))
      .filter((x) => !!x);

    this.possibleValues = _.sortedUniq(
      values.filter((v) => v.toLowerCase().includes(filterValue.toLowerCase()))
    );
    const keyValueArray = this.possibleValues.map((item) => ({ content: item, value: item }));
    this.allPossibleValues = keyValueArray;
  }

  getMode() {
    return this.editMode ? this.actionLabels.EDIT : this.actionLabels.ADD;
  }

  preFillControls(matcher: AlertmanagerSilenceMatcher) {
    if (!this.form) return;
    this.setPossibleValues(matcher.name);
    this.form.get('value')?.enable({ emitEvent: false });
    this.form.patchValue({
      name: matcher.name,
      value: matcher.value,
      isRegex: matcher.isRegex ?? false
    });
    this.cdRef.detectChanges();
  }

  onSubmit() {
    if (this.form.invalid) return;
    this.submitAction.emit(this.form.getRawValue());
    this.closeModal();
  }
}
