import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { FeatureTogglesService } from '~/app/shared/services/feature-toggles.service';

import { configureTestBed } from '~/testing/unit-test-helper';
import { DashboardComponent } from './dashboard.component';

describe('DashboardComponent', () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let featureTogglesService: FeatureTogglesService;

  configureTestBed({
    imports: [NgbNavModule, HttpClientTestingModule],
    declarations: [DashboardComponent],
    providers: [FeatureTogglesService],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    featureTogglesService = TestBed.inject(FeatureTogglesService);
    fixture = TestBed.createComponent(DashboardComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    spyOn(featureTogglesService, 'isFeatureEnabled').and.returnValue(true);

    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should call featureTogglesService.isFeatureEnabled() on initialization', () => {
    const spy = spyOn(featureTogglesService, 'isFeatureEnabled').and.returnValue(true);
    fixture.detectChanges();
    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledWith('dashboard');
  });

  it('should set useDeprecated based on feature toggle', () => {
    spyOn(featureTogglesService, 'isFeatureEnabled').and.returnValue(true);
    fixture.detectChanges();
    expect(component.useDeprecated).toBe(true);
  });

  describe('when dashboard feature is enabled (new dashboard)', () => {
    beforeEach(() => {
      spyOn(featureTogglesService, 'isFeatureEnabled').and.returnValue(true);
      fixture.detectChanges();
    });

    it('should show cd-dashboard-v3 in template when dashboard feature is enabled', () => {
      const overviewElement = fixture.debugElement.query(By.css('[data-testid="cd-overview"]'));
      const dashboardV3Element = fixture.debugElement.query(
        By.css('[data-testid="cd-dashboard-v3"]')
      );

      expect(overviewElement).toBeNull();
      expect(dashboardV3Element).toBeTruthy();
    });

    it('should set useDeprecated to false when feature is enabled', () => {
      expect(component.useDeprecated).toBe(true);
    });
  });

  describe('when dashboard feature is disabled (old dashboard)', () => {
    beforeEach(() => {
      spyOn(featureTogglesService, 'isFeatureEnabled').and.returnValue(false);
      fixture.detectChanges();
    });

    it('should show cd-overview in template when dashboard feature is disabled', () => {
      const overviewElement = fixture.debugElement.query(By.css('[data-testid="cd-overview"]'));
      const dashboardV3Element = fixture.debugElement.query(
        By.css('[data-testid="cd-dashboard-v3"]')
      );

      expect(overviewElement).toBeTruthy();
      expect(dashboardV3Element).toBeNull();
    });

    it('should set useDeprecated to true when feature is disabled', () => {
      expect(component.useDeprecated).toBe(false);
    });
  });
});
