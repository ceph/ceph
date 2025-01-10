import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbJoinAuthFormComponent } from './smb-join-auth-form.component';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { ReactiveFormsModule } from '@angular/forms';

describe('SmbJoinAuthFormComponent', () => {
  let component: SmbJoinAuthFormComponent;
  let fixture: ComponentFixture<SmbJoinAuthFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ToastrModule.forRoot(), SharedModule, ReactiveFormsModule],
      declarations: [SmbJoinAuthFormComponent],
      providers: [provideHttpClient(), provideHttpClientTesting(), provideRouter([])]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbJoinAuthFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
