import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbShareListComponent } from './smb-share-list.component';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideHttpClient } from '@angular/common/http';
import { ActivatedRoute } from '@angular/router';
import { SMB_BASE_CEPHFS } from '../smb-route.util';

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
        {
          provide: ActivatedRoute,
          useValue: {
            pathFromRoot: [{ snapshot: { data: { smbBasePath: SMB_BASE_CEPHFS, isRgw: false } } }]
          }
        }
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
