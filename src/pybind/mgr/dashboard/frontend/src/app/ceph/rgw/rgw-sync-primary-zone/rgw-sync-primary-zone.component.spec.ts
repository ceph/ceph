import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSyncPrimaryZoneComponent } from './rgw-sync-primary-zone.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { By } from '@angular/platform-browser';

describe('RgwSyncPrimaryZoneComponent', () => {
  let component: RgwSyncPrimaryZoneComponent;
  let fixture: ComponentFixture<RgwSyncPrimaryZoneComponent>;

  configureTestBed({
    declarations: [RgwSyncPrimaryZoneComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwSyncPrimaryZoneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display realm, zonegroup, and zone in badges', () => {
    component.realm = 'Realm';
    component.zonegroup = 'Zonegroup';
    component.zone = 'Zone';
    fixture.detectChanges();

    const realmBadge = fixture.debugElement.query(By.css('li:nth-child(2)'));
    expect(realmBadge.nativeElement.textContent).toContain('Realm');

    const zonegroupBadge = fixture.debugElement.query(By.css('p'));
    expect(zonegroupBadge.nativeElement.textContent).toContain('Zonegroup');

    const zoneBadge = fixture.debugElement.query(By.css('li:nth-child(8)'));
    expect(zoneBadge.nativeElement.textContent).toContain('Zone');
  });
});
