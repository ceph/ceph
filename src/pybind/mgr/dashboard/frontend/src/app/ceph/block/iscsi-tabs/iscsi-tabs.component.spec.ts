import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { IscsiTabsComponent } from './iscsi-tabs.component';
import { By } from '@angular/platform-browser';

describe('IscsiTabsComponent', () => {
  let component: IscsiTabsComponent;
  let fixture: ComponentFixture<IscsiTabsComponent>;

  configureTestBed({
    imports: [SharedModule, RouterTestingModule],
    declarations: [IscsiTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display the heading text in the tab', () => {
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.length).toBe(2);
    expect(tabs[0].attributes['heading']).toBe('Overview');
    expect(tabs[1].attributes['heading']).toBe('Targets');
  });
});
