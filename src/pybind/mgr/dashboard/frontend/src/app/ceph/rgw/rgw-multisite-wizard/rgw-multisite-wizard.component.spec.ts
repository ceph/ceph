import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteWizardComponent } from './rgw-multisite-wizard.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ReactiveFormsModule } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';

describe('RgwMultisiteWizardComponent', () => {
  let component: RgwMultisiteWizardComponent;
  let fixture: ComponentFixture<RgwMultisiteWizardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteWizardComponent],
      imports: [HttpClientTestingModule, SharedModule, ReactiveFormsModule, ToastrModule.forRoot()],
      providers: [NgbActiveModal]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteWizardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
