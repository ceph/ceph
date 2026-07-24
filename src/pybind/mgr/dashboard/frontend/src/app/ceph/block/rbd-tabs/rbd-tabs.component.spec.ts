import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RbdTabsComponent } from './rbd-tabs.component';
import { By } from '@angular/platform-browser';

describe('RbdTabsComponent', () => {
  let component: RbdTabsComponent;
  let fixture: ComponentFixture<RbdTabsComponent>;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [RbdTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display the heading text in the tab', () => {
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.length).toBeGreaterThanOrEqual(3);
    expect(tabs[0].attributes['heading']).toBe('Images');
    expect(tabs[1].attributes['heading']).toBe('Namespaces');
    expect(tabs[2].attributes['heading']).toBe('Trash');
  });
});
