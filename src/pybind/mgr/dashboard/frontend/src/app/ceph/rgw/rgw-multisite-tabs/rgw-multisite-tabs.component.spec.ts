import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { RgwMultisiteTabsComponent } from './rgw-multisite-tabs.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { By } from '@angular/platform-browser';

describe('RgwMultisiteTabsComponent', () => {
  let component: RgwMultisiteTabsComponent;
  let fixture: ComponentFixture<RgwMultisiteTabsComponent>;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [RgwMultisiteTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display the heading text in the tab', () => {
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.length).toBe(2);
    expect(tabs[0].attributes['heading']).toBe('Configuration');
    expect(tabs[1].attributes['heading']).toBe('Sync Policy');
  });
});
