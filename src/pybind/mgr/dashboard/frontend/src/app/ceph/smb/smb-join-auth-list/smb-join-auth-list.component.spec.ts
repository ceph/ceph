import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbJoinAuthListComponent } from './smb-join-auth-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { SMB_CEPHFS_CONTEXT, SMB_CONTEXT } from '~/app/shared/constants/smb.constant';

describe('SmbJoinAuthListComponent', () => {
  let component: SmbJoinAuthListComponent;
  let fixture: ComponentFixture<SmbJoinAuthListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbJoinAuthListComponent],
      imports: [SharedModule, HttpClientTestingModule, RouterTestingModule],
      providers: [{ provide: SMB_CONTEXT, useValue: SMB_CEPHFS_CONTEXT }]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbJoinAuthListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
