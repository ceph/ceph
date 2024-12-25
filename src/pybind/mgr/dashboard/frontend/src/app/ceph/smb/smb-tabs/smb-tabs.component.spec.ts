import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbTabsComponent } from './smb-tabs.component';

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
});
