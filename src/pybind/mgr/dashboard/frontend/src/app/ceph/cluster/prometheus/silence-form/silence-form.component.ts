import { Component } from '@angular/core';
import { Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';
import moment from 'moment';

import { DashboardNotFoundError } from '~/app/core/error/error';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { ActionLabelsI18n, SucceededActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import {
  AlertmanagerSilence,
  AlertmanagerSilenceMatcher,
  AlertmanagerSilenceMatcherMatch
} from '~/app/shared/models/alertmanager-silence';
import { Permission } from '~/app/shared/models/permissions';
import { AlertmanagerAlert, PrometheusRule } from '~/app/shared/models/prometheus-alerts';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { PrometheusSilenceMatcherService } from '~/app/shared/services/prometheus-silence-matcher.service';
import { TimeDiffService } from '~/app/shared/services/time-diff.service';
import { SilenceMatcherModalComponent } from '../silence-matcher-modal/silence-matcher-modal.component';

@Component({
  selector: 'cd-prometheus-form',
  templateUrl: './silence-form.component.html',
  styleUrls: ['./silence-form.component.scss']
})
export class SilenceFormComponent {
  icons = Icons;
  permission: Permission;
  form: CdFormGroup;
  rules: PrometheusRule[];
  matchName = '';
  matchValue = '';

  recreate = false;
  edit = false;
  id: string;

  action: string;
  resource = $localize`silence`;

  matchers: AlertmanagerSilenceMatcher[] = [];
  matcherMatch: AlertmanagerSilenceMatcherMatch = undefined;
  matcherConfig = [
    {
      tooltip: $localize`Attribute name`,
      attribute: 'name'
    },
    {
      tooltip: $localize`Regular expression`,
      attribute: 'isRegex'
    },
    {
      tooltip: $localize`Value`,
      attribute: 'value'
    }
  ];

  datetimeFormat = 'YYYY-MM-DD HH:mm';
  isNavigate = true;

  constructor(
    private router: Router,
    private authStorageService: AuthStorageService,
    private formBuilder: CdFormBuilder,
    private prometheusService: PrometheusService,
    private notificationService: NotificationService,
    private route: ActivatedRoute,
    private timeDiff: TimeDiffService,
    private modalService: ModalService,
    private silenceMatcher: PrometheusSilenceMatcherService,
    private actionLabels: ActionLabelsI18n,
    private succeededLabels: SucceededActionLabelsI18n
  ) {
    this.init();
  }

  private init() {
    this.chooseMode();
    this.authenticate();
    this.createForm();
    this.setupDates();
    this.getData();
  }

  private chooseMode() {
    this.edit = this.router.url.startsWith('/monitoring/silences/edit');
    this.recreate = this.router.url.startsWith('/monitoring/silences/recreate');
    if (this.edit) {
      this.action = this.actionLabels.EDIT;
    } else if (this.recreate) {
      this.action = this.actionLabels.RECREATE;
    } else {
      this.action = this.actionLabels.CREATE;
    }
  }

  private authenticate() {
    this.permission = this.authStorageService.getPermissions().prometheus;
    const allowed =
      this.permission.read && (this.edit ? this.permission.update : this.permission.create);
    if (!allowed) {
      throw new DashboardNotFoundError();
    }
  }

  private createForm() {
    const formatValidator = CdValidators.custom('format', (expiresAt: string) => {
      const result = expiresAt === '' || moment(expiresAt, this.datetimeFormat).isValid();
      return !result;
    });
    this.form = this.formBuilder.group(
      {
        startsAt: ['', [Validators.required, formatValidator]],
        duration: ['2h', [Validators.min(1)]],
        endsAt: ['', [Validators.required, formatValidator]],
        createdBy: [this.authStorageService.getUsername(), [Validators.required]],
        comment: [null, [Validators.required]]
      },
      {
        validators: CdValidators.custom('matcherRequired', () => this.matchers.length === 0)
      }
    );
  }

  private setupDates() {
    const now = moment().format(this.datetimeFormat);
    this.form.silentSet('startsAt', now);
    this.updateDate();
    this.subscribeDateChanges();
  }

  private updateDate(updateStartDate?: boolean) {
    const date = moment(
      this.form.getValue(updateStartDate ? 'endsAt' : 'startsAt'),
      this.datetimeFormat
    ).toDate();
    const next = this.timeDiff.calculateDate(date, this.form.getValue('duration'), updateStartDate);
    if (next) {
      const nextDate = moment(next).format(this.datetimeFormat);
      this.form.silentSet(updateStartDate ? 'startsAt' : 'endsAt', nextDate);
    }
  }

  private subscribeDateChanges() {
    this.form.get('startsAt').valueChanges.subscribe(() => {
      this.onDateChange();
    });
    this.form.get('duration').valueChanges.subscribe(() => {
      this.updateDate();
    });
    this.form.get('endsAt').valueChanges.subscribe(() => {
      this.onDateChange(true);
    });
  }

  private onDateChange(updateStartDate?: boolean) {
    const startsAt = moment(this.form.getValue('startsAt'), this.datetimeFormat);
    const endsAt = moment(this.form.getValue('endsAt'), this.datetimeFormat);
    if (startsAt.isBefore(endsAt)) {
      this.updateDuration();
    } else {
      this.updateDate(updateStartDate);
    }
  }

  private updateDuration() {
    const startsAt = moment(this.form.getValue('startsAt'), this.datetimeFormat).toDate();
    const endsAt = moment(this.form.getValue('endsAt'), this.datetimeFormat).toDate();
    this.form.silentSet('duration', this.timeDiff.calculateDuration(startsAt, endsAt));
  }

  private getData() {
    this.getRules();
    this.getModeSpecificData();
  }

  getRules() {
    this.prometheusService.ifPrometheusConfigured(
      () =>
        this.prometheusService.getRules().subscribe(
          (groups) => {
            this.rules = groups['groups'].reduce(
              (acc, group) => _.concat<PrometheusRule>(acc, group.rules),
              []
            );
          },
          () => {
            this.prometheusService.disablePrometheusConfig();
            this.rules = [];
          }
        ),
      () => {
        this.rules = [];
        this.notificationService.show(
          NotificationType.info,
          $localize`Please add your Prometheus host to the dashboard configuration and refresh the page`,
          undefined,
          undefined,
          'Prometheus'
        );
      }
    );
    return this.rules;
  }

  private getModeSpecificData() {
    this.route.params.subscribe((params: { id: string }) => {
      if (!params.id) {
        return;
      }
      if (this.edit || this.recreate) {
        this.prometheusService.getSilences().subscribe((silences) => {
          const silence = _.find(silences, ['id', params.id]);
          if (!_.isUndefined(silence)) {
            this.fillFormWithSilence(silence);
          }
        });
      } else {
        this.prometheusService.getAlerts().subscribe((alerts) => {
          const alert = _.find(alerts, ['fingerprint', params.id]);
          if (!_.isUndefined(alert)) {
            this.fillFormByAlert(alert);
          }
        });
      }
    });
  }

  private fillFormWithSilence(silence: AlertmanagerSilence) {
    this.id = silence.id;
    if (this.edit) {
      ['startsAt', 'endsAt'].forEach((attr) =>
        this.form.silentSet(attr, moment(silence[attr]).format(this.datetimeFormat))
      );
      this.updateDuration();
    }
    ['createdBy', 'comment'].forEach((attr) => this.form.silentSet(attr, silence[attr]));
    this.matchers = silence.matchers;
    this.validateMatchers();
  }

  private validateMatchers() {
    if (!this.rules) {
      window.setTimeout(() => this.validateMatchers(), 100);
      return;
    }
    this.matcherMatch = this.silenceMatcher.multiMatch(this.matchers, this.rules);
    this.form.markAsDirty();
    this.form.updateValueAndValidity();
  }

  private fillFormByAlert(alert: AlertmanagerAlert) {
    const labels = alert.labels;
    this.setMatcher({
      name: 'alertname',
      value: labels.alertname,
      isRegex: false
    });
  }

  private setMatcher(matcher: AlertmanagerSilenceMatcher, index?: number) {
    if (_.isNumber(index)) {
      this.matchers[index] = matcher;
    } else {
      this.matchers.push(matcher);
    }
    this.validateMatchers();
  }

  showMatcherModal(index?: number) {
    const modalRef = this.modalService.show(SilenceMatcherModalComponent);
    const modalComponent = modalRef.componentInstance as SilenceMatcherModalComponent;
    modalComponent.rules = this.rules;
    if (_.isNumber(index)) {
      modalComponent.editMode = true;
      modalComponent.preFillControls(this.matchers[index]);
    }
    modalComponent.submitAction.subscribe((matcher: AlertmanagerSilenceMatcher) => {
      this.setMatcher(matcher, index);
    });
  }

  deleteMatcher(index: number) {
    this.matchers.splice(index, 1);
    this.validateMatchers();
  }

  submit(data?: any) {
    if (this.form.invalid) {
      return;
    }
    this.prometheusService.setSilence(this.getSubmitData()).subscribe(
      (resp) => {
        if (data) {
          data.silenceId = resp.body['silenceId'];
        }
        if (this.isNavigate) {
          this.router.navigate(['/monitoring/silences']);
        }
        this.notificationService.show(
          NotificationType.success,
          this.getNotificationTile(this.matchers),
          undefined,
          undefined,
          'Prometheus'
        );
        this.matchers = [];
      },
      () => this.form.setErrors({ cdSubmitButton: true })
    );
  }

  private getSubmitData(): AlertmanagerSilence {
    const payload = this.form.value;
    delete payload.duration;
    payload.startsAt = moment(payload.startsAt, this.datetimeFormat).toISOString();
    payload.endsAt = moment(payload.endsAt, this.datetimeFormat).toISOString();
    payload.matchers = this.matchers;
    if (this.edit) {
      payload.id = this.id;
    }
    return payload;
  }

  private getNotificationTile(matchers: AlertmanagerSilenceMatcher[]) {
    let action;
    if (this.edit) {
      action = this.succeededLabels.EDITED;
    } else if (this.recreate) {
      action = this.succeededLabels.RECREATED;
    } else {
      action = this.succeededLabels.CREATED;
    }
    let msg = '';
    for (const matcher of matchers) {
      msg = msg.concat(` ${matcher.name} - ${matcher.value},`);
    }
    return `${action} ${this.resource} for ${msg.slice(0, -1)}`;
  }
}
