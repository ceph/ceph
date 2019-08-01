import { DatePipe } from '@angular/common';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CdIndividualConfig } from '../../models/cd-notification';
import { CdDatePipe } from '../../pipes/cd-date.pipe';
import { ToastFooterComponent } from './toast-footer.component';

describe('ToastTimeApplicationComponent', () => {
  let component: ToastFooterComponent;
  let fixture: ComponentFixture<ToastFooterComponent>;

  configureTestBed({
    declarations: [ToastFooterComponent, CdDatePipe],
    providers: [DatePipe]
  });

  const configCeph: Partial<CdIndividualConfig> = {
    application: 'Ceph',
    timestamp: new Date().toJSON()
  };

  const configPrometheus: Partial<CdIndividualConfig> = {
    application: 'Prometheus',
    timestamp: new Date().toJSON()
  };

  beforeEach(() => {
    fixture = TestBed.createComponent(ToastFooterComponent);
    component = fixture.componentInstance;
    component.config = configCeph as CdIndividualConfig;
    component.ngOnInit();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set a timestamp', () => {
    expect(component.timestamp).toBeDefined();
  });

  describe('ToastTimeApplicationComponent Ceph application', () => {
    beforeEach(() => {
      fixture = TestBed.createComponent(ToastFooterComponent);
      component = fixture.componentInstance;
      component.config = configCeph as CdIndividualConfig;
      component.ngOnInit();
    });

    it('should set application correctly', () => {
      expect(component.application).toBeDefined();
      expect(component.application).toBe('Ceph');
    });

    it('should set applicationClass correctly', () => {
      expect(component.applicationClass).toBeDefined();
      expect(component.applicationClass).toBe('ceph-icon');
    });
  });

  describe('ToastTimeApplicationComponent Prometheus application', () => {
    beforeEach(() => {
      fixture = TestBed.createComponent(ToastFooterComponent);
      component = fixture.componentInstance;
      component.config = configPrometheus as CdIndividualConfig;
      component.ngOnInit();
    });

    it('should set a Prometheus application', () => {
      expect(component.application).toBeDefined();
      expect(component.application).toBe('Prometheus');
    });

    it('should set a Prometheus applicationClass', () => {
      expect(component.applicationClass).toBeDefined();
      expect(component.applicationClass).toBe('prometheus-icon');
    });
  });
});
