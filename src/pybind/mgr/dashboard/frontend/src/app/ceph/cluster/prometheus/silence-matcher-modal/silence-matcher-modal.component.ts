import { Component, EventEmitter, Output, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { NgbActiveModal, NgbTypeahead } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';
import { merge, Observable, Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, map } from 'rxjs/operators';

import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import {
  AlertmanagerSilenceMatcher,
  AlertmanagerSilenceMatcherMatch
} from '../../../../shared/models/alertmanager-silence';
import { PrometheusRule } from '../../../../shared/models/prometheus-alerts';
import { PrometheusSilenceMatcherService } from '../../../../shared/services/prometheus-silence-matcher.service';

@Component({
  selector: 'cd-silence-matcher-modal',
  templateUrl: './silence-matcher-modal.component.html',
  styleUrls: ['./silence-matcher-modal.component.scss']
})
export class SilenceMatcherModalComponent {
  @ViewChild(NgbTypeahead, { static: true })
  typeahead: NgbTypeahead;
  @Output()
  submitAction = new EventEmitter();

  form: CdFormGroup;
  editMode = false;
  rules: PrometheusRule[];
  nameAttributes = ['alertname', 'instance', 'job', 'severity'];
  possibleValues: string[] = [];
  matcherMatch: AlertmanagerSilenceMatcherMatch = undefined;

  // For typeahead usage
  valueClick = new Subject<string>();
  valueFocus = new Subject<string>();
  search = (text$: Observable<string>) => {
    return merge(
      text$.pipe(debounceTime(200), distinctUntilChanged()),
      this.valueFocus,
      this.valueClick.pipe(filter(() => !this.typeahead.isPopupOpen()))
    ).pipe(
      map((term) =>
        (term === ''
          ? this.possibleValues
          : this.possibleValues.filter((v) => v.toLowerCase().indexOf(term.toLowerCase()) > -1)
        ).slice(0, 10)
      )
    );
  };

  constructor(
    private formBuilder: CdFormBuilder,
    private silenceMatcher: PrometheusSilenceMatcherService,
    public activeModal: NgbActiveModal
  ) {
    this.createForm();
    this.subscribeToChanges();
  }

  private createForm() {
    this.form = this.formBuilder.group({
      name: [null, [Validators.required]],
      value: [{ value: '', disabled: true }, [Validators.required]],
      isRegex: new FormControl(false)
    });
  }

  private subscribeToChanges() {
    this.form.get('name').valueChanges.subscribe((name) => {
      if (name === null) {
        this.form.get('value').disable();
        return;
      }
      this.setPossibleValues(name);
      this.form.get('value').enable();
    });
    this.form.get('value').valueChanges.subscribe((value) => {
      const values = this.form.value;
      values.value = value; // Isn't the current value at this stage
      this.matcherMatch = this.silenceMatcher.singleMatch(values, this.rules);
    });
  }

  private setPossibleValues(name: string) {
    this.possibleValues = _.sortedUniq(
      this.rules.map((r) => _.get(r, this.silenceMatcher.getAttributePath(name))).filter((x) => x)
    );
  }

  preFillControls(matcher: AlertmanagerSilenceMatcher) {
    this.form.setValue(matcher);
  }

  onSubmit() {
    this.submitAction.emit(this.form.value);
    this.activeModal.close();
  }
}
