import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { LicenceAgreementComponent } from './license-agreement.component';

jest.mock('jspdf', () => ({
  jsPDF: jest.fn().mockImplementation(() => ({
    text: jest.fn(),
    addImage: jest.fn(),
    save: jest.fn()
  }))
}));

describe('LicenceAgreementComponent', () => {
  let component: LicenceAgreementComponent;
  let fixture: ComponentFixture<LicenceAgreementComponent>;

  configureTestBed({
    declarations: [LicenceAgreementComponent],
    imports: [FormsModule, ToastrModule.forRoot(), SharedModule, ReactiveFormsModule],
    providers: [provideHttpClient(), provideHttpClientTesting(), provideRouter([])]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LicenceAgreementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
