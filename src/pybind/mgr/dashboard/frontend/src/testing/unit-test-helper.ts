import { LOCALE_ID, TRANSLATIONS, TRANSLATIONS_FORMAT, Type } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { AbstractControl } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { configureTestSuite } from 'ng-bullet';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { TableActionsComponent } from '../app/shared/datatable/table-actions/table-actions.component';
import { Icons } from '../app/shared/enum/icons.enum';
import { CdFormGroup } from '../app/shared/forms/cd-form-group';
import { CdTableAction } from '../app/shared/models/cd-table-action';
import { CdTableSelection } from '../app/shared/models/cd-table-selection';
import { Permission } from '../app/shared/models/permissions';
import {
  AlertmanagerAlert,
  AlertmanagerNotification,
  AlertmanagerNotificationAlert,
  PrometheusRule
} from '../app/shared/models/prometheus-alerts';

export function configureTestBed(configuration: any) {
  configureTestSuite(() => TestBed.configureTestingModule(configuration));
}

export class PermissionHelper {
  tac: TableActionsComponent;
  permission: Permission;

  constructor(permission: Permission) {
    this.permission = permission;
  }

  setPermissionsAndGetActions(tableActions: CdTableAction[]): any {
    const result = {};
    [true, false].forEach((create) => {
      [true, false].forEach((update) => {
        [true, false].forEach((deleteP) => {
          this.permission.create = create;
          this.permission.update = update;
          this.permission.delete = deleteP;

          this.tac = new TableActionsComponent();
          this.tac.selection = new CdTableSelection();
          this.tac.tableActions = [...tableActions];
          this.tac.permission = this.permission;
          this.tac.ngOnInit();

          const perms = [];
          if (create) {
            perms.push('create');
          }
          if (update) {
            perms.push('update');
          }
          if (deleteP) {
            perms.push('delete');
          }
          const permissionText = perms.join(',');

          result[permissionText !== '' ? permissionText : 'no-permissions'] = {
            actions: this.tac.tableActions.map((action) => action.name),
            primary: this.testScenarios()
          };
        });
      });
    });

    return result;
  }

  testScenarios() {
    const result: any = {};
    // 'multiple selections'
    result.multiple = this.testScenario([{}, {}]);
    // 'select executing item'
    result.executing = this.testScenario([{ cdExecuting: 'someAction' }]);
    // 'select non-executing item'
    result.single = this.testScenario([{}]);
    // 'no selection'
    result.no = this.testScenario([]);

    return result;
  }

  private testScenario(selection: object[]) {
    this.setSelection(selection);
    const btn = this.tac.getCurrentButton();
    return btn ? btn.name : '';
  }

  setSelection(selection: object[]) {
    this.tac.selection.selected = selection;
  }
}

export class FormHelper {
  form: CdFormGroup;

  constructor(form: CdFormGroup) {
    this.form = form;
  }

  /**
   * Changes multiple values in multiple controls
   */
  setMultipleValues(values: { [controlName: string]: any }, markAsDirty?: boolean) {
    Object.keys(values).forEach((key) => {
      this.setValue(key, values[key], markAsDirty);
    });
  }

  /**
   * Changes the value of a control
   */
  setValue(control: AbstractControl | string, value: any, markAsDirty?: boolean): AbstractControl {
    control = this.getControl(control);
    if (markAsDirty) {
      control.markAsDirty();
    }
    control.setValue(value);
    return control;
  }

  private getControl(control: AbstractControl | string): AbstractControl {
    if (typeof control === 'string') {
      return this.form.get(control);
    }
    return control;
  }

  /**
   * Change the value of the control and expect the control to be valid afterwards.
   */
  expectValidChange(control: AbstractControl | string, value: any, markAsDirty?: boolean) {
    this.expectValid(this.setValue(control, value, markAsDirty));
  }

  /**
   * Expect that the given control is valid.
   */
  expectValid(control: AbstractControl | string) {
    // 'isValid' would be false for disabled controls
    expect(this.getControl(control).errors).toBe(null);
  }

  /**
   * Change the value of the control and expect a specific error.
   */
  expectErrorChange(
    control: AbstractControl | string,
    value: any,
    error: string,
    markAsDirty?: boolean
  ) {
    this.expectError(this.setValue(control, value, markAsDirty), error);
  }

  /**
   * Expect a specific error for the given control.
   */
  expectError(control: AbstractControl | string, error: string) {
    expect(this.getControl(control).hasError(error)).toBeTruthy();
  }
}

/**
 * Use this to mock 'ModalService.show' to make the embedded component with it's fixture usable
 * in tests. The function gives back all needed parts including the modal reference.
 *
 * Please make sure to call this function *inside* your mock and return the reference at the end.
 */
export function modalServiceShow(componentClass: Type<any>, modalConfig: any) {
  const ref = new BsModalRef();
  const fixture = TestBed.createComponent(componentClass);
  let component = fixture.componentInstance;
  if (modalConfig.initialState) {
    component = Object.assign(component, modalConfig.initialState);
  }
  fixture.detectChanges();
  ref.content = component;
  return { ref, fixture, component };
}

export class FixtureHelper {
  fixture: ComponentFixture<any>;

  constructor(fixture?: ComponentFixture<any>) {
    if (fixture) {
      this.updateFixture(fixture);
    }
  }

  updateFixture(fixture: ComponentFixture<any>) {
    this.fixture = fixture;
  }

  /**
   * Expect a list of id elements to be visible or not.
   */
  expectIdElementsVisible(ids: string[], visibility: boolean) {
    ids.forEach((css) => {
      this.expectElementVisible(`#${css}`, visibility);
    });
  }

  /**
   * Expect a specific element to be visible or not.
   */
  expectElementVisible(css: string, visibility: boolean) {
    expect(visibility).toBe(Boolean(this.getElementByCss(css)));
  }

  expectFormFieldToBe(css: string, value: string) {
    const props = this.getElementByCss(css).properties;
    expect(props['value'] || props['checked'].toString()).toBe(value);
  }

  expectTextToBe(css: string, value: string) {
    expect(this.getText(css)).toBe(value);
  }

  clickElement(css: string) {
    this.getElementByCss(css).triggerEventHandler('click', null);
    this.fixture.detectChanges();
  }

  selectElement(css: string, value: string) {
    const nativeElement = this.getElementByCss(css).nativeElement;
    nativeElement.value = value;
    nativeElement.dispatchEvent(new Event('change'));
    this.fixture.detectChanges();
  }

  getText(css: string) {
    const e = this.getElementByCss(css);
    return e ? e.nativeElement.textContent.trim() : null;
  }

  getTextAll(css: string) {
    const elements = this.getElementByCssAll(css);
    return elements.map((element) => {
      return element ? element.nativeElement.textContent.trim() : null;
    });
  }

  getElementByCss(css: string) {
    this.fixture.detectChanges();
    return this.fixture.debugElement.query(By.css(css));
  }

  getElementByCssAll(css: string) {
    this.fixture.detectChanges();
    return this.fixture.debugElement.queryAll(By.css(css));
  }
}

export class PrometheusHelper {
  createSilence(id: string) {
    return {
      id: id,
      createdBy: `Creator of ${id}`,
      comment: `A comment for ${id}`,
      startsAt: new Date('2022-02-22T22:22:00').toISOString(),
      endsAt: new Date('2022-02-23T22:22:00').toISOString(),
      matchers: [
        {
          name: 'job',
          value: 'someJob',
          isRegex: true
        }
      ]
    };
  }

  createRule(name: string, severity: string, alerts: any[]): PrometheusRule {
    return {
      name: name,
      labels: {
        severity: severity
      },
      alerts: alerts
    } as PrometheusRule;
  }

  createAlert(name: string, state = 'active', timeMultiplier = 1): AlertmanagerAlert {
    return {
      fingerprint: name,
      status: { state },
      labels: {
        alertname: name,
        instance: 'someInstance',
        job: 'someJob',
        severity: 'someSeverity'
      },
      annotations: {
        summary: `${name} is ${state}`
      },
      generatorURL: `http://${name}`,
      startsAt: new Date(new Date('2022-02-22').getTime() * timeMultiplier).toString()
    } as AlertmanagerAlert;
  }

  createNotificationAlert(name: string, status = 'firing'): AlertmanagerNotificationAlert {
    return {
      status: status,
      labels: {
        alertname: name
      },
      annotations: {
        summary: `${name} is ${status}`
      },
      generatorURL: `http://${name}`
    } as AlertmanagerNotificationAlert;
  }

  createNotification(alertNumber = 1, status = 'firing'): AlertmanagerNotification {
    const alerts = [];
    for (let i = 0; i < alertNumber; i++) {
      alerts.push(this.createNotificationAlert('alert' + i, status));
    }
    return { alerts, status } as AlertmanagerNotification;
  }

  createLink(url: string) {
    return `<a href="${url}" target="_blank"><i class="${Icons.lineChart}"></i></a>`;
  }
}

const XLIFF = `<?xml version="1.0" encoding="UTF-8" ?>
<xliff version="1.2" xmlns="urn:oasis:names:tc:xliff:document:1.2">
  <file source-language="en" datatype="plaintext" original="ng2.template">
    <body>
    </body>
  </file>
</xliff>
`;

const i18nProviders = [
  { provide: TRANSLATIONS_FORMAT, useValue: 'xlf' },
  { provide: TRANSLATIONS, useValue: XLIFF },
  { provide: LOCALE_ID, useValue: 'en' },
  I18n
];

export { i18nProviders };

export function expectItemTasks(item: any, executing: string, percentage?: number) {
  if (executing) {
    executing = executing + '...';
    if (percentage) {
      executing = `${executing} ${percentage}%`;
    }
  }
  expect(item.cdExecuting).toBe(executing);
}

export class IscsiHelper {
  static validateUser(formHelper: FormHelper, fieldName: string) {
    formHelper.expectErrorChange(fieldName, 'short', 'pattern');
    formHelper.expectValidChange(fieldName, 'thisIsCorrect');
    formHelper.expectErrorChange(fieldName, '##?badChars?##', 'pattern');
    formHelper.expectErrorChange(
      fieldName,
      'thisUsernameIsWayyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyTooBig',
      'pattern'
    );
  }

  static validatePassword(formHelper: FormHelper, fieldName: string) {
    formHelper.expectErrorChange(fieldName, 'short', 'pattern');
    formHelper.expectValidChange(fieldName, 'thisIsCorrect');
    formHelper.expectErrorChange(fieldName, '##?badChars?##', 'pattern');
    formHelper.expectErrorChange(fieldName, 'thisPasswordIsWayTooBig', 'pattern');
  }
}
