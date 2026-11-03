import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbUsersgroupsListComponent } from './smb-usersgroups-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { SMB_CEPHFS_CONTEXT, SMB_CONTEXT } from '~/app/shared/constants/smb.constant';

describe('SmbUsersgroupsListComponent', () => {
  let component: SmbUsersgroupsListComponent;
  let fixture: ComponentFixture<SmbUsersgroupsListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbUsersgroupsListComponent],
      imports: [SharedModule, HttpClientTestingModule, RouterTestingModule],
      providers: [{ provide: SMB_CONTEXT, useValue: SMB_CEPHFS_CONTEXT }]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
