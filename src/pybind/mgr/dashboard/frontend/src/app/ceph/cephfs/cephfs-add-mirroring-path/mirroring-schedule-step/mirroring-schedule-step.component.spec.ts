import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { MirroringScheduleStepComponent } from './mirroring-schedule-step.component';
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';

describe('MirroringScheduleStepComponent', () => {
  let component: MirroringScheduleStepComponent;
  let fixture: ComponentFixture<MirroringScheduleStepComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [MirroringScheduleStepComponent],
      imports: [ReactiveFormsModule],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(MirroringScheduleStepComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(MirroringScheduleStepComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize form group with default values on ngOnInit', () => {
    component.ngOnInit();

    expect(component.formGroup).toBeTruthy();
    const values = component.formGroup.getRawValue();
    expect(values.repeatInterval).toBe(1);
    expect(values.repeatFrequency).toBe(RepeatFrequency.Daily);
    expect(values.startDate).toBe('');
    expect(values.retentionInterval).toBe(7);
    expect(values.retentionFrequency).toBe(RetentionFrequency.Daily);
    expect(values.retentionCount).toBe(5);
  });

  it('should expose repeat and retention frequency entries', () => {
    expect(component.repeatFrequencies.length).toBeGreaterThan(0);
    expect(component.retentionFrequencies.length).toBeGreaterThan(0);
    expect(component.repeatFrequencies).toContainEqual(['Daily', RepeatFrequency.Daily]);
    expect(component.retentionFrequencies).toContainEqual(['Daily', RetentionFrequency.Daily]);
  });

  describe('buildCreatePayload', () => {
    beforeEach(() => {
      component.ngOnInit();
    });

    it('should build payload with default form values', () => {
      const payload = component.buildCreatePayload('testfs', { path: '/volumes/g1/sv1' });

      expect(payload.fs).toBe('testfs');
      expect(payload.path).toBe('/volumes/g1/sv1');
      expect(payload.snap_schedule).toBe('1d');
      expect(payload.start).toBeTruthy();
      expect(payload.retention_policy).toBe('7-d|5-n');
      expect(payload.subvol).toBeUndefined();
      expect(payload.group).toBeUndefined();
    });

    it('should include subvol and group when provided', () => {
      const payload = component.buildCreatePayload('testfs', {
        path: '/volumes/g1/sv1',
        subvol: 'sv1',
        group: 'g1'
      });

      expect(payload.subvol).toBe('sv1');
      expect(payload.group).toBe('g1');
    });

    it('should include subvol without group when group is not provided', () => {
      const payload = component.buildCreatePayload('testfs', {
        path: '/volumes/_nogroup/sv1',
        subvol: 'sv1'
      });

      expect(payload.subvol).toBe('sv1');
      expect(payload.group).toBeUndefined();
    });

    it('should reflect custom form values in the payload', () => {
      component.formGroup.patchValue({
        repeatInterval: 4,
        repeatFrequency: RepeatFrequency.Hourly,
        retentionInterval: 14,
        retentionFrequency: RetentionFrequency.Weekly,
        retentionCount: 10
      });

      const payload = component.buildCreatePayload('myfs', { path: '/data' });

      expect(payload.fs).toBe('myfs');
      expect(payload.path).toBe('/data');
      expect(payload.snap_schedule).toBe('4h');
      expect(payload.retention_policy).toBe('14-w|10-n');
    });

    it('should omit retention_policy when retention values are falsy', () => {
      component.formGroup.patchValue({
        retentionInterval: 0,
        retentionFrequency: '',
        retentionCount: 0
      });

      const payload = component.buildCreatePayload('testfs', { path: '/volumes/g1/sv1' });

      expect(payload.retention_policy).toBeUndefined();
    });

    it('should include startDate in the payload when set', () => {
      component.formGroup.patchValue({ startDate: '2025-06-15' });

      const payload = component.buildCreatePayload('testfs', { path: '/volumes/g1/sv1' });

      expect(payload.start).toContain('2025-06-15');
    });

    it('should default start to current time when startDate is empty', () => {
      const before = new Date().toISOString().slice(0, 13);

      const payload = component.buildCreatePayload('testfs', { path: '/volumes/g1/sv1' });

      expect(payload.start.slice(0, 13)).toBe(before);
    });
  });

  describe('form validation', () => {
    beforeEach(() => {
      component.ngOnInit();
    });

    it('should be valid with default values', () => {
      expect(component.formGroup.valid).toBe(true);
    });

    it('should be invalid when repeatInterval is less than 1', () => {
      component.formGroup.patchValue({ repeatInterval: 0 });

      expect(component.formGroup.get('repeatInterval').valid).toBe(false);
      expect(component.formGroup.get('repeatInterval').hasError('min')).toBe(true);
    });

    it('should be invalid when retentionInterval is less than 1', () => {
      component.formGroup.patchValue({ retentionInterval: 0 });

      expect(component.formGroup.get('retentionInterval').valid).toBe(false);
    });

    it('should be invalid when retentionCount is less than 1', () => {
      component.formGroup.patchValue({ retentionCount: 0 });

      expect(component.formGroup.get('retentionCount').valid).toBe(false);
    });
  });
});
