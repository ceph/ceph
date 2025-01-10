import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbUsersgroupsFormComponent } from './smb-usersgroups-form.component';
import { ToastrModule } from 'ngx-toastr';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { provideRouter } from '@angular/router';
import { SharedModule } from '~/app/shared/shared.module';

describe('SmbUsersgroupsFormComponent', () => {
  let component: SmbUsersgroupsFormComponent;
  let fixture: ComponentFixture<SmbUsersgroupsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ToastrModule.forRoot(), SharedModule, ReactiveFormsModule],
      declarations: [SmbUsersgroupsFormComponent],
      providers: [provideHttpClient(), provideHttpClientTesting(), provideRouter([])]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
