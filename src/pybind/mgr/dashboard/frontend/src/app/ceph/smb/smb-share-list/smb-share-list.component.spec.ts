import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbShareListComponent } from './smb-share-list.component';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideHttpClient } from '@angular/common/http';
import { SMB_CEPHFS_CONTEXT, SMB_CONTEXT } from '~/app/shared/constants/smb.constant';

import { SharedModule } from '~/app/shared/shared.module';

describe('SmbShareListComponent', () => {
  let component: SmbShareListComponent;
  let fixture: ComponentFixture<SmbShareListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SharedModule],
      declarations: [SmbShareListComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: SMB_CONTEXT, useValue: SMB_CEPHFS_CONTEXT }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbShareListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
