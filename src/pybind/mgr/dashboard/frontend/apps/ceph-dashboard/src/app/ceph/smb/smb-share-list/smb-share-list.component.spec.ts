import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbShareListComponent } from './smb-share-list.component';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideHttpClient } from '@angular/common/http';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';

describe('SmbShareListComponent', () => {
  let component: SmbShareListComponent;
  let fixture: ComponentFixture<SmbShareListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ToastrModule.forRoot(), SharedModule],
      declarations: [SmbShareListComponent],
      providers: [provideHttpClient(), provideHttpClientTesting()]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbShareListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
