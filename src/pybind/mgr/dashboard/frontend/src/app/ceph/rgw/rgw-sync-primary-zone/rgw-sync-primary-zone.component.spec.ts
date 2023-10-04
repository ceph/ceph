import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSyncPrimaryZoneComponent } from './rgw-sync-primary-zone.component';
import { configureTestBed } from '~/testing/unit-test-helper';

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
});
