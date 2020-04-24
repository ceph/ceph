import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { AlertModule } from 'ngx-bootstrap/alert';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { CdForm } from '../forms/cd-form';
import { SharedModule } from '../shared.module';
import { FormLoadingDirective } from './form-loading.directive';

@Component({ selector: 'cd-test-cmp', template: '<span *cdFormLoading="loading">foo</span>' })
class TestComponent extends CdForm {
  constructor() {
    super();
  }
}

describe('FormLoadingDirective', () => {
  let component: TestComponent;
  let fixture: ComponentFixture<any>;

  const expectShown = (elem: number, error: number, loading: number) => {
    expect(fixture.debugElement.queryAll(By.css('span')).length).toEqual(elem);
    expect(fixture.debugElement.queryAll(By.css('cd-alert-panel')).length).toEqual(error);
    expect(fixture.debugElement.queryAll(By.css('cd-loading-panel')).length).toEqual(loading);
  };

  configureTestBed({
    declarations: [TestComponent],
    imports: [AlertModule.forRoot(), SharedModule],
    providers: [i18nProviders]
  });

  afterEach(() => {
    fixture = null;
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create an instance', () => {
    const directive = new FormLoadingDirective(null, null, null, null);
    expect(directive).toBeTruthy();
  });

  it('should show loading component by default', () => {
    expectShown(0, 0, 1);

    const alert = fixture.debugElement.nativeElement.querySelector('cd-loading-panel alert');
    expect(alert.textContent).toBe('Loading form data...');
  });

  it('should show error component when calling loadingError()', () => {
    component.loadingError();
    fixture.detectChanges();

    expectShown(0, 1, 0);

    const alert = fixture.debugElement.nativeElement.querySelector(
      'cd-alert-panel .alert-panel-text'
    );
    expect(alert.textContent).toBe('Form data could not be loaded.');
  });

  it('should show original component when calling loadingReady()', () => {
    component.loadingReady();
    fixture.detectChanges();

    expectShown(1, 0, 0);

    const alert = fixture.debugElement.nativeElement.querySelector('span');
    expect(alert.textContent).toBe('foo');
  });

  it('should show nothing when calling loadingNone()', () => {
    component.loadingNone();
    fixture.detectChanges();

    expectShown(0, 0, 0);
  });
});
