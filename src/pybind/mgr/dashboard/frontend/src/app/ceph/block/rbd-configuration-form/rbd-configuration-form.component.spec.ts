import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { ReplaySubject } from 'rxjs';

import { DirectivesModule } from '~/app/shared/directives/directives.module';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { RbdConfigurationSourceField } from '~/app/shared/models/configuration';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { RbdConfigurationService } from '~/app/shared/services/rbd-configuration.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { RbdConfigurationFormComponent } from './rbd-configuration-form.component';
import { ButtonModule, InputModule } from 'carbon-components-angular';

describe('RbdConfigurationFormComponent', () => {
  let component: RbdConfigurationFormComponent;
  let fixture: ComponentFixture<RbdConfigurationFormComponent>;
  let sections: any[];
  let fh: FormHelper;

  configureTestBed({
    imports: [ReactiveFormsModule, DirectivesModule, SharedModule, InputModule, ButtonModule],
    declarations: [RbdConfigurationFormComponent],
    providers: [RbdConfigurationService, FormatterService, DimlessBinaryPerSecondPipe]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdConfigurationFormComponent);
    component = fixture.componentInstance;
    component.form = new CdFormGroup({}, null);
    fh = new FormHelper(component.form);
    fixture.detectChanges();
    sections = TestBed.inject(RbdConfigurationService).sections;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should create all form fields mentioned in RbdConfiguration::OPTIONS', () => {
    /* Test form creation on a TypeScript level */
    const actual = Object.keys((component.form.get('configuration') as CdFormGroup).controls);
    const expected = sections
      .map((section) => section.options)
      .reduce((a, b) => a.concat(b))
      .map((option: Record<string, any>) => option.name);
    expect(actual).toEqual(expected);

    /* Test form creation on a template level */
    const controlDebugElements = fixture.debugElement.queryAll(By.css('input'));
    expect(controlDebugElements.length).toBe(expected.length);
    controlDebugElements.forEach((element) => expect(element.nativeElement).toBeTruthy());
  });

  it('should only contain values of changed controls if submitted', () => {
    let values = {};
    component.changes.subscribe((getDirtyValues: Function) => {
      values = getDirtyValues();
    });
    fh.setValue('configuration.rbd_qos_bps_limit', 0, true);
    fixture.detectChanges();

    expect(values).toEqual({ rbd_qos_bps_limit: 0 });
  });

  describe('test loading of initial data for editing', () => {
    beforeEach(() => {
      component.initializeData = new ReplaySubject<any>(1);
      fixture.detectChanges();
      component.ngOnInit();
    });

    it('should return dirty values without any units', () => {
      let dirtyValues = {};
      component.changes.subscribe((getDirtyValues: Function) => {
        dirtyValues = getDirtyValues();
      });

      fh.setValue('configuration.rbd_qos_bps_limit', 55, true);
      fh.setValue('configuration.rbd_qos_iops_limit', 22, true);

      expect(dirtyValues['rbd_qos_bps_limit']).toBe(55);
      expect(dirtyValues['rbd_qos_iops_limit']).toBe(22);
    });

    it('should load initial data into forms', () => {
      component.initializeData.next({
        initialData: [
          {
            name: 'rbd_qos_bps_limit',
            value: 55,
            source: 1
          }
        ],
        sourceType: RbdConfigurationSourceField.pool
      });

      expect(component.form.getValue('configuration.rbd_qos_bps_limit')).toEqual('55 B/s');
    });

    it('should not load initial data if the source is not the pool itself', () => {
      component.initializeData.next({
        initialData: [
          {
            name: 'rbd_qos_bps_limit',
            value: 55,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_iops_limit',
            value: 22,
            source: RbdConfigurationSourceField.global
          }
        ],
        sourceType: RbdConfigurationSourceField.pool
      });

      expect(component.form.getValue('configuration.rbd_qos_iops_limit')).toEqual('0 IOPS');
      expect(component.form.getValue('configuration.rbd_qos_bps_limit')).toEqual('0 B/s');
    });

    it('should not load initial data if the source is not the image itself', () => {
      component.initializeData.next({
        initialData: [
          {
            name: 'rbd_qos_bps_limit',
            value: 55,
            source: RbdConfigurationSourceField.pool
          },
          {
            name: 'rbd_qos_iops_limit',
            value: 22,
            source: RbdConfigurationSourceField.global
          }
        ],
        sourceType: RbdConfigurationSourceField.image
      });

      expect(component.form.getValue('configuration.rbd_qos_iops_limit')).toEqual('0 IOPS');
      expect(component.form.getValue('configuration.rbd_qos_bps_limit')).toEqual('0 B/s');
    });

    it('should always have formatted results', () => {
      component.initializeData.next({
        initialData: [
          {
            name: 'rbd_qos_bps_limit',
            value: 55,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_iops_limit',
            value: 22,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_read_bps_limit',
            value: null, // incorrect type
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_read_bps_limit',
            value: undefined, // incorrect type
            source: RbdConfigurationSourceField.image
          }
        ],
        sourceType: RbdConfigurationSourceField.image
      });

      expect(component.form.getValue('configuration.rbd_qos_iops_limit')).toEqual('22 IOPS');
      expect(component.form.getValue('configuration.rbd_qos_bps_limit')).toEqual('55 B/s');
      expect(component.form.getValue('configuration.rbd_qos_read_bps_limit')).toEqual('0 B/s');
      expect(component.form.getValue('configuration.rbd_qos_read_bps_limit')).toEqual('0 B/s');
    });
  });

  it('should reset the corresponding form field correctly', () => {
    const fieldName = 'rbd_qos_bps_limit';
    const getValue = () => component.form.get(`configuration.${fieldName}`).value;

    // Initialization
    fh.setValue(`configuration.${fieldName}`, 418, true);
    expect(getValue()).toBe(418);

    // Reset
    component.reset(fieldName);
    expect(getValue()).toBe(null);

    // Restore
    component.reset(fieldName);
    expect(getValue()).toBe(418);

    // Reset
    component.reset(fieldName);
    expect(getValue()).toBe(null);

    // Restore
    component.reset(fieldName);
    expect(getValue()).toBe(418);
  });

  describe('should verify that getDirtyValues() returns correctly', () => {
    let data: any;

    beforeEach(() => {
      component.initializeData = new ReplaySubject<any>(1);
      fixture.detectChanges();
      component.ngOnInit();
      data = {
        initialData: [
          {
            name: 'rbd_qos_bps_limit',
            value: 0,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_iops_limit',
            value: 0,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_read_bps_limit',
            value: 0,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_read_iops_limit',
            value: 0,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_read_iops_burst',
            value: 0,
            source: RbdConfigurationSourceField.image
          },
          {
            name: 'rbd_qos_write_bps_burst',
            value: undefined,
            source: RbdConfigurationSourceField.global
          },
          {
            name: 'rbd_qos_write_iops_burst',
            value: null,
            source: RbdConfigurationSourceField.global
          }
        ],
        sourceType: RbdConfigurationSourceField.image
      };
      component.initializeData.next(data);
    });

    it('should return an empty object', () => {
      expect(component.getDirtyValues()).toEqual({});
      expect(component.getDirtyValues(true, RbdConfigurationSourceField.image)).toEqual({});
    });

    it('should return dirty values', () => {
      component.form.get('configuration.rbd_qos_write_bps_burst').markAsDirty();
      expect(component.getDirtyValues()).toEqual({ rbd_qos_write_bps_burst: 0 });

      component.form.get('configuration.rbd_qos_write_iops_burst').markAsDirty();
      expect(component.getDirtyValues()).toEqual({
        rbd_qos_write_iops_burst: 0,
        rbd_qos_write_bps_burst: 0
      });
    });

    it('should also return all local values if they do not contain their initial values', () => {
      // Change value for all options
      data.initialData = data.initialData.map((o: Record<string, any>) => {
        o.value = 22;
        return o;
      });

      // Mark some dirty
      ['rbd_qos_read_iops_limit', 'rbd_qos_write_bps_burst'].forEach((option) => {
        component.form.get(`configuration.${option}`).markAsDirty();
      });

      expect(component.getDirtyValues(true, RbdConfigurationSourceField.image)).toEqual({
        rbd_qos_read_iops_limit: 0,
        rbd_qos_write_bps_burst: 0
      });
    });

    it('should throw an error if used incorrectly', () => {
      expect(() => component.getDirtyValues(true)).toThrowError(
        /^ProgrammingError: If local values shall be included/
      );
    });
  });
});
