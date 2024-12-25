import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbUsersgroupsDetailsComponent } from './smb-usersgroups-details.component';

describe('SmbUsersgroupsDetailsComponent', () => {
  let component: SmbUsersgroupsDetailsComponent;
  let fixture: ComponentFixture<SmbUsersgroupsDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbUsersgroupsDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
