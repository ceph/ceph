import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbClusterTabsComponent } from './smb-cluster-tabs.component';

describe('SmbClusterTabsComponent', () => {
  let component: SmbClusterTabsComponent;
  let fixture: ComponentFixture<SmbClusterTabsComponent>;

  configureTestBed({
    declarations: [SmbClusterTabsComponent]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(SmbClusterTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
