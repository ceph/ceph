import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbJoinAuthFormComponent } from './smb-join-auth-form.component';

describe('SmbJoinAuthFormComponent', () => {
  let component: SmbJoinAuthFormComponent;
  let fixture: ComponentFixture<SmbJoinAuthFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SmbJoinAuthFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SmbJoinAuthFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
