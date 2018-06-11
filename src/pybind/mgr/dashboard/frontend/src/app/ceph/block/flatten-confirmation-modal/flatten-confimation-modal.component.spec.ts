import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { ApiModule } from '../../../shared/api/api.module';
import { ServicesModule } from '../../../shared/services/services.module';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { FlattenConfirmationModalComponent } from './flatten-confimation-modal.component';

describe('FlattenConfirmationModalComponent', () => {
  let component: FlattenConfirmationModalComponent;
  let fixture: ComponentFixture<FlattenConfirmationModalComponent>;

  configureTestBed({
    imports: [
      ReactiveFormsModule,
      HttpClientTestingModule,
      SharedModule,
      ServicesModule,
      ApiModule,
      ToastModule.forRoot()
    ],
    declarations: [FlattenConfirmationModalComponent],
    providers: [BsModalRef, BsModalService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FlattenConfirmationModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
