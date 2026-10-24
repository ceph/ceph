import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbClusterListComponent } from './smb-cluster-list.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { SMB_CEPHFS_CONTEXT, SMB_CONTEXT } from '~/app/shared/constants/smb.constant';

describe('SmbClusterListComponent', () => {
  let component: SmbClusterListComponent;
  let fixture: ComponentFixture<SmbClusterListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule
      ],
      declarations: [SmbClusterListComponent],
      providers: [{ provide: SMB_CONTEXT, useValue: SMB_CEPHFS_CONTEXT }]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
