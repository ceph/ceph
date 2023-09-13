import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSyncPrimaryZoneComponent } from './rgw-sync-primary-zone.component';

describe('RgwSyncPrimaryZoneComponent', () => {
  let component: RgwSyncPrimaryZoneComponent;
  let fixture: ComponentFixture<RgwSyncPrimaryZoneComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwSyncPrimaryZoneComponent]
    }).compileComponents();
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
