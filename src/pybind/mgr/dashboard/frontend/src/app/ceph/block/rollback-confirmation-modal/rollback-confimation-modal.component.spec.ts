import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { ApiModule } from '../../../shared/api/api.module';
import { ServicesModule } from '../../../shared/services/services.module';
import { SharedModule } from '../../../shared/shared.module';
import { RollbackConfirmationModalComponent } from './rollback-confimation-modal.component';

describe('RollbackConfirmationModalComponent', () => {
  let component: RollbackConfirmationModalComponent;
  let fixture: ComponentFixture<RollbackConfirmationModalComponent>;

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
      declarations: [ RollbackConfirmationModalComponent ],
      providers: [ BsModalRef, BsModalService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RollbackConfirmationModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
