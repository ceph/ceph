import { Component, OnInit } from '@angular/core';
import { Validators } from '@angular/forms';
import { Router } from '@angular/router';

import * as _ from 'lodash';
import { defineLocale } from 'ngx-bootstrap/chronos';
import { BsLocaleService } from 'ngx-bootstrap/datepicker';
import { deLocale, ptBrLocale } from 'ngx-bootstrap/locale';
import { BsModalService } from 'ngx-bootstrap/modal';

import { LocaleHelper } from '../../../../locale.helper';
import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../../shared/forms/cd-validators';
import { Permission } from '../../../../shared/models/permissions';
import { PrometheusSilenceMatcher } from '../../../../shared/models/prometheus-silence';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { SilenceMatcherModalComponent } from '../silence-matcher-modal/silence-matcher-modal.component';
import { AlertmanagerAlert, PrometheusRule } from '../../../../shared/models/prometheus-alerts';

@Component({
  selector: 'cd-prometheus-form',
  templateUrl: './silence-form.component.html',
  styleUrls: ['./silence-form.component.scss']
})
export class SilenceFormComponent implements OnInit {
  permission: Permission;
  form: CdFormGroup;
  edit = false;
  recreate = false;
  matchers: PrometheusSilenceMatcher[] = [];
  // Date formatting rules can be found here: https://momentjs.com/docs/#/displaying/format/
  // The problem with localised dates is that manual edits often end up as invalid date
  // bsConfig = { dateInputFormat: 'llll' };
  bsConfig = { dateInputFormat: 'YYYY-MM-DDT HH:mm' };
  matcherConfig = [
    {
      tooltip: 'Matcher name',
      icon: 'paragraph',
      attribute: 'name'
    },
    {
      tooltip: 'Prometheus expression',
      icon: 'terminal',
      attribute: 'value'
    },
    {
      tooltip: 'Regular expression',
      icon: 'magic',
      attribute: 'isRegex'
    }
  ];

  alerts: AlertmanagerAlert[];
  rules: PrometheusRule[];

  constructor(
    private prometheusService: PrometheusService,
    private formBuilder: CdFormBuilder,
    private authStorageService: AuthStorageService,
    private localeService: BsLocaleService,
    private bsModalService: BsModalService,
    private router: Router
  ) {
    this.chooseMode();
    this.authenticate();
    this.getData();
    this.createForm();
    this.setupDates();
  }

  chooseMode() {
    this.edit = this.router.url.startsWith('/silence/edit');
    this.recreate = this.router.url.startsWith('/silence/recreate');
  }

  authenticate() {
    this.permission = this.authStorageService.getPermissions().prometheus;
    if (
      !this.permission.read ||
      ((!this.permission.update && this.edit) || (!this.permission.create && !this.edit))
    ) {
      this.router.navigate(['/404']);
    }
  }

  private getData() {
    this.prometheusService.ifPrometheusConfigured(() =>
      this.prometheusService.getRules().subscribe((rules) => (this.rules = rules))
    );
    this.prometheusService.getAlerts().subscribe((alerts) => (this.alerts = alerts));
  }

  private createForm() {
    this.form = this.formBuilder.group(
      {
        // Date configuration
        startsAt: [null, [Validators.required]],
        duration: ['2h', [Validators.min(1)]], // validate if wrong? because wrong inputs will be ignored!
        endsAt: [null, [Validators.required]],
        // Creator and comment
        createdBy: [null, [Validators.required]],
        comment: [null, [Validators.required]]
      },
      {
        validators: CdValidators.custom('matcherRequired', (rule) => this.matchers.length === 0)
      }
    );
  }

  private setupDates() {
    const now = new Date();
    now.setSeconds(0, 0);
    this.form.silentSet('startsAt', now);
    this.recalculateDate();

    // can be removed if locals are not needed
    defineLocale('de', deLocale);
    defineLocale('pt', ptBrLocale);
    this.localeService.use(LocaleHelper.getLocale().slice(0, 2));

    this.form.get('startsAt').valueChanges.subscribe(() => {
      this.onDateChange();
    });
    this.form.get('duration').valueChanges.subscribe(() => {
      this.recalculateDate();
    });
    this.form.get('endsAt').valueChanges.subscribe(() => {
      this.onDateChange(true);
    });
  }

  private onDateChange(setStart?: boolean) {
    if (+this.form.getValue('startsAt') < +this.form.getValue('endsAt')) {
      this.changeDuration();
    } else {
      this.recalculateDate(setStart);
    }
  }

  private changeDuration() {
    const endTime = +this.form.getValue('endsAt');
    const startTime = +this.form.getValue('startsAt');
    if (startTime > endTime) {
      throw new Error('That should be implemented');
    }
    this.form.silentSet('duration', this.makeDuration(endTime - startTime));
  }

  makeDuration(ms: number): string {
    const date = new Date(ms);
    const h = date.getUTCHours();
    const m = date.getUTCMinutes();

    const minutes = 60 * 1000;
    const hours = 60 * minutes;
    const days = 24 * hours;
    const d = Math.floor(ms / days);

    const format = (n, s) => (n ? n + s : n);
    return [format(d, 'd'), format(h, 'h'), format(m, 'm')].filter((x) => x).join(' ');
  }

  private recalculateDate(setStart?: boolean) {
    const time = +this.form.getValue(setStart ? 'endsAt' : 'startsAt');
    if (_.isNaN(time)) {
      return;
    }
    const duration = this.getDurationMs(this.form.getValue('duration')) * (setStart ? -1 : 1);
    this.form.silentSet(setStart ? 'startsAt' : 'endsAt', new Date(time + duration));
  }

  getDurationMs(duration: string): number {
    const d = this.getNumbersFromString(duration, 'd');
    const h = this.getNumbersFromString(duration, 'h');
    const m = this.getNumbersFromString(duration, 'm');
    return ((d * 24 + h) * 60 + m) * 60000;
  }

  private getNumbersFromString(duration, prefix): number {
    const match = duration.match(new RegExp(`[0-9 ]+${prefix}`, 'i'));
    return match ? parseInt(match, 10) : 0;
  }

  ngOnInit() {}

  deleteMatcher(index: number) {
    this.matchers.splice(index, 1);
    this.form.updateValueAndValidity();
  }

  showMatcherModal(index?: number) {
    const modalRef = this.bsModalService.show(SilenceMatcherModalComponent);
    const modal = modalRef.content as SilenceMatcherModalComponent;
    modal.alerts = this.alerts;
    modal.rules = this.rules;
    if (_.isNumber(index)) {
      modal.preFillControls(this.matchers[index]);
    }
    modalRef.content.submitAction.subscribe((matcher: PrometheusSilenceMatcher) => {
      this.setMatcher(matcher, index);
    });
  }

  setMatcher(matcher: PrometheusSilenceMatcher, index?: number) {
    if (_.isNumber(index)) {
      this.matchers[index] = matcher;
    } else {
      this.matchers.push(matcher);
    }
    // Mark the form as dirty to be able to submit it.
    this.form.markAsDirty();
    this.form.updateValueAndValidity();
  }

  submit() {
    const payload = this.form.value;
    payload.matchers = this.matchers;
    console.log('hi');
    this.prometheusService.setSilence(payload).subscribe(
      () => this.router.navigate(['/silence']),
      (resp) => {
        resp['application'] = 'Prometheus';
      }
    );
  }
}
