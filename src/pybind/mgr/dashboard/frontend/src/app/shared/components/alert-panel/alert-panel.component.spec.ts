import { ComponentFixture, TestBed } from '@angular/core/testing';
import { AlertComponent, AlertConfig } from 'ngx-bootstrap/alert';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { AlertPanelComponent } from './alert-panel.component';

describe('AlertPanelComponent', () => {
  let component: AlertPanelComponent;
  let fixture: ComponentFixture<AlertPanelComponent>;

  configureTestBed({
    declarations: [AlertPanelComponent, AlertComponent],
    providers: [AlertConfig, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AlertPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
