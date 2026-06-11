import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { RgwUserTabsComponent } from './rgw-user-tabs.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { By } from '@angular/platform-browser';

describe('RgwUserTabsComponent', () => {
  let component: RgwUserTabsComponent;
  let fixture: ComponentFixture<RgwUserTabsComponent>;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [RgwUserTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display the heading text in the tab', () => {
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.length).toBe(3);
    expect(tabs[0].attributes['heading']).toBe('Users');
    expect(tabs[1].attributes['heading']).toBe('Accounts');
    expect(tabs[2].attributes['heading']).toBe('Roles');
  });
});
