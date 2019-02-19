import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { SettingRegistryService } from '../../../shared/api/setting-registry.service';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { GrafanaTimesConstants } from '../../../shared/constants/grafanaTimes.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-setting-form',
  templateUrl: './setting-form.component.html',
  styleUrls: ['./setting-form.component.scss']
})
export class SettingFormComponent implements OnInit {
  settingForm: CdFormGroup;
  obj: any[];
  settingName: string;
  actionForm: string;
  actionButton: string;
  uiSettings = {};
  loading = true;
  icons = Icons;

  constructor(
    private settingRegistryService: SettingRegistryService,
    private route: ActivatedRoute,
    private i18n: I18n,
    private formBuilder: CdFormBuilder,
    private router: Router,
    private notificationService: NotificationService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.route.params.subscribe((params: { name: string }) => {
      this.settingName = params.name;
      this.settingRegistryService.getSettingsList().subscribe((data) => {
        data.forEach((d) => (this.uiSettings[d['name']] = d));
        this.setData(this.uiSettings[this.settingName]);
        this.loading = false;
        this.createForm();
      });
    });
  }

  ngOnInit() {}

  createForm() {
    const formControls = {};
    _.forEach(this.obj, (setting) => {
      formControls[setting.id] = [setting.value];
    });

    this.settingForm = this.formBuilder.group(formControls);
    this.actionForm = this.actionLabels.EDIT + ' ' + this.settingName;
    this.actionButton = this.actionLabels.EDIT;
  }

  setData(data) {
    switch (data.name) {
      case 'grafana': {
        this.obj = [
          {
            id: 'name',
            label: 'Name',
            value: data.name,
            readonly: true,
            type: 'text'
          },
          {
            id: 'refresh_interval',
            label: 'Refresh Interval',
            value: parseInt(data.refresh_interval, 10),
            readonly: false,
            type: 'number'
          },
          {
            id: 'timepicker',
            label: 'Default timepicker',
            value: data.timepicker,
            readonly: false,
            type: 'select',
            options: GrafanaTimesConstants.grafanaTimeData
          }
        ];
      }
    }
  }

  goToListView() {
    this.router.navigate(['/settings']);
  }

  submit() {
    if (this.settingForm.pristine) {
      this.goToListView();
      return;
    }
    const config = {};
    _.forEach(this.obj, (setting) => {
      const control = this.settingForm.get(setting.id);
      config[setting.id] = control.value;
    });

    this.settingRegistryService.updateSetting(this.settingName, config).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          this.i18n('Updated options for setting "{{settingName}}".', {
            settingName: this.settingName
          })
        );
        this.settingRegistryService.getSettingsList().subscribe((data) => {
          const uiSettings = {};
          data.forEach((d) => (uiSettings[d['name']] = d));
          localStorage.setItem(`ui-settings`, JSON.stringify(uiSettings));
        });
        this.goToListView();
      },
      () => {
        // Reset the 'Submit' button.
        this.settingForm.setErrors({ cdSubmitButton: true });
      }
    );
  }
}
