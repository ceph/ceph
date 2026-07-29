import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HealthChecksComponent } from './health-checks.component';
import { HealthColorPipe } from '~/app/shared/pipes/health-color.pipe';
import { CssHelper } from '~/app/shared/classes/css-helper';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

describe('HealthChecksComponent', () => {
  let component: HealthChecksComponent;
  let fixture: ComponentFixture<HealthChecksComponent>;

  configureTestBed({
    declarations: [HealthChecksComponent, HealthColorPipe],
    providers: [CssHelper],
    schemas: [CUSTOM_ELEMENTS_SCHEMA]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(HealthChecksComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
