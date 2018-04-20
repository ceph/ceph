import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { ApiModule } from '../../../shared/api/api.module';
import { ServicesModule } from '../../../shared/services/services.module';
import { SharedModule } from '../../../shared/shared.module';
import { FlattenConfirmationModalComponent } from './flatten-confimation-modal.component';

describe('FlattenConfirmationModalComponent', () => {
  let component: FlattenConfirmationModalComponent;
  let fixture: ComponentFixture<FlattenConfirmationModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        ReactiveFormsModule,
        HttpClientTestingModule,
        SharedModule,
        ServicesModule,
        ApiModule,
        ToastModule.forRoot()
      ],
      declarations: [ FlattenConfirmationModalComponent ],
      providers: [ BsModalRef, BsModalService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FlattenConfirmationModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
