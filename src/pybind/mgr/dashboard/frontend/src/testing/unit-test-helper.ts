import { DebugElement, LOCALE_ID, TRANSLATIONS, TRANSLATIONS_FORMAT, Type } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { AbstractControl } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { configureTestSuite } from 'ng-bullet';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { NgbNav, NgbNavItem } from '@ng-bootstrap/ng-bootstrap';
import { TableActionsComponent } from '../app/shared/datatable/table-actions/table-actions.component';
import { Icons } from '../app/shared/enum/icons.enum';
import { CdFormGroup } from '../app/shared/forms/cd-form-group';
import { CdTableAction } from '../app/shared/models/cd-table-action';
import { CdTableSelection } from '../app/shared/models/cd-table-selection';
import { CrushNode } from '../app/shared/models/crush-node';
import { CrushRule, CrushRuleConfig } from '../app/shared/models/crush-rule';
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

export class Mocks {
  static getCrushNode(
    name: string,
    id: number,
    type: string,
    type_id: number,
    children?: number[],
    device_class?: string
  ): CrushNode {
    return { name, type, type_id, id, children, device_class };
  }

  /**
   * Create the following test crush map:
   * > default
   * --> ssd-host
   * ----> 3x osd with ssd
   * --> mix-host
   * ----> hdd-rack
   * ------> 2x osd-rack with hdd
   * ----> ssd-rack
   * ------> 2x osd-rack with ssd
   */
  static getCrushMap(): CrushNode[] {
    return [
      // Root node
      this.getCrushNode('default', -1, 'root', 11, [-2, -3]),
      // SSD host
      this.getCrushNode('ssd-host', -2, 'host', 1, [1, 0, 2]),
      this.getCrushNode('osd.0', 0, 'osd', 0, undefined, 'ssd'),
      this.getCrushNode('osd.1', 1, 'osd', 0, undefined, 'ssd'),
      this.getCrushNode('osd.2', 2, 'osd', 0, undefined, 'ssd'),
      // SSD and HDD mixed devices host
      this.getCrushNode('mix-host', -3, 'host', 1, [-4, -5]),
      // HDD rack
      this.getCrushNode('hdd-rack', -4, 'rack', 3, [3, 4]),
      this.getCrushNode('osd2.0', 3, 'osd-rack', 0, undefined, 'hdd'),
      this.getCrushNode('osd2.1', 4, 'osd-rack', 0, undefined, 'hdd'),
      // SSD rack
      this.getCrushNode('ssd-rack', -5, 'rack', 3, [5, 6]),
      this.getCrushNode('osd3.0', 5, 'osd-rack', 0, undefined, 'ssd'),
      this.getCrushNode('osd3.1', 6, 'osd-rack', 0, undefined, 'ssd')
    ];
  }

  /**
   * Generates an simple crush map with multiple hosts that have OSDs with either ssd or hdd OSDs.
   * Hosts with zero or even numbers at the end have SSD OSDs the other hosts have hdd OSDs.
   *
   * Host names follow the following naming convention:
   * host.$index
   * $index represents a number count started at 0 (like an index within an array) (same for OSDs)
   *
   * OSD names follow the following naming convention:
   * osd.$hostIndex.$osdIndex
   *
   * The following crush map will be generated with the set defaults:
   * > default
   * --> host.0 (has only ssd OSDs)
   * ----> osd.0.0
   * ----> osd.0.1
   * ----> osd.0.2
   * ----> osd.0.3
   * --> host.1 (has only hdd OSDs)
   * ----> osd.1.0
   * ----> osd.1.1
   * ----> osd.1.2
   * ----> osd.1.3
   */
  static generateSimpleCrushMap(hosts: number = 2, osds: number = 4): CrushNode[] {
    const nodes = [];
    const createOsdLeafs = (hostSuffix: number): number[] => {
      let osdId = 0;
      const osdIds = [];
      const osdsInUse = hostSuffix * osds;
      for (let o = 0; o < osds; o++) {
        osdIds.push(osdId);
        nodes.push(
          this.getCrushNode(
            `osd.${hostSuffix}.${osdId}`,
            osdId + osdsInUse,
            'osd',
            0,
            undefined,
            hostSuffix % 2 === 0 ? 'ssd' : 'hdd'
          )
        );
        osdId++;
      }
      return osdIds;
    };
    const createHostBuckets = (): number[] => {
      let hostId = -2;
      const hostIds = [];
      for (let h = 0; h < hosts; h++) {
        const hostSuffix = hostId * -1 - 2;
        hostIds.push(hostId);
        nodes.push(
          this.getCrushNode(`host.${hostSuffix}`, hostId, 'host', 1, createOsdLeafs(hostSuffix))
        );
        hostId--;
      }
      return hostIds;
    };
    nodes.push(this.getCrushNode('default', -1, 'root', 11, createHostBuckets()));
    return nodes;
  }

  static getCrushRuleConfig(
    name: string,
    root: string,
    failure_domain: string,
    device_class?: string
  ): CrushRuleConfig {
    return {
      name,
      root,
      failure_domain,
      device_class
    };
  }

  static getCrushRule({
    id = 0,
    name = 'somePoolName',
    min = 1,
    max = 10,
    type = 'replicated',
    failureDomain = 'osd',
    itemName = 'default' // This string also sets the device type - "default~ssd" <- ssd usage only
  }: {
    max?: number;
    min?: number;
    id?: number;
    name?: string;
    type?: string;
    failureDomain?: string;
    itemName?: string;
  }): CrushRule {
    const typeNumber = type === 'erasure' ? 3 : 1;
    const rule = new CrushRule();
    rule.max_size = max;
    rule.min_size = min;
    rule.rule_id = id;
    rule.ruleset = typeNumber;
    rule.rule_name = name;
    rule.steps = [
      {
        item_name: itemName,
        item: -1,
        op: 'take'
      },
      {
        num: 0,
        type: failureDomain,
        op: 'choose_firstn'
      },
      {
        op: 'emit'
      }
    ];
    return rule;
  }
}

export class TabHelper {
  static getNgbNav(fixture: ComponentFixture<any>) {
    const debugElem: DebugElement = fixture.debugElement;
    return debugElem.query(By.directive(NgbNav)).injector.get(NgbNav);
  }

  static getNgbNavItems(fixture: ComponentFixture<any>) {
    const debugElems = this.getNgbNavItemsDebugElems(fixture);
    return debugElems.map((de) => de.injector.get(NgbNavItem));
  }

  static getTextContents(fixture: ComponentFixture<any>) {
    const debugElems = this.getNgbNavItemsDebugElems(fixture);
    return debugElems.map((de) => de.nativeElement.textContent);
  }

  private static getNgbNavItemsDebugElems(fixture: ComponentFixture<any>) {
    const debugElem: DebugElement = fixture.debugElement;
    return debugElem.queryAll(By.directive(NgbNavItem));
  }
}
