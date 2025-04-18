import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbClusterTabsComponent } from './smb-cluster-tabs.component';

describe('SmbClusterTabsComponent', () => {
  let component: SmbClusterTabsComponent;
  let fixture: ComponentFixture<SmbClusterTabsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbClusterTabsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
