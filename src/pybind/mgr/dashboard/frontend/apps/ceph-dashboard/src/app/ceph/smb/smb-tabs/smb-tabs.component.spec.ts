import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbTabsComponent } from './smb-tabs.component';
import { By } from '@angular/platform-browser';

describe('SmbTabsComponent', () => {
  let component: SmbTabsComponent;
  let fixture: ComponentFixture<SmbTabsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbTabsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display the heading text in the tab', () => {
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.length).toBe(3);
    expect(tabs[0].attributes['heading']).toBe('Clusters');
    expect(tabs[1].attributes['heading']).toBe('Active Directory');
    expect(tabs[2].attributes['heading']).toBe('Standalone');
  });

  // If the tabs cacheActive is set to true data for all tabs will be fetched at once,
  // smb mgr module might hit recursive error when doing multiple request to the db
  it('should have cache disabled', () => {
    const tabs = fixture.nativeElement.querySelector('cds-tabs');
    expect(tabs.cacheActive).toBeFalsy();
  });
});
