import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbUsersgroupsFormComponent } from './smb-usersgroups-form.component';

describe('SmbUsersgroupsFormComponent', () => {
  let component: SmbUsersgroupsFormComponent;
  let fixture: ComponentFixture<SmbUsersgroupsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SmbUsersgroupsFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
