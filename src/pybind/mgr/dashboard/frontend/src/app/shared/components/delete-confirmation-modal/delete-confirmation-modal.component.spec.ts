import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { ApiModule } from '../../../shared/api/api.module';
import { ServicesModule } from '../../../shared/services/services.module';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { DeleteConfirmationComponent } from './delete-confirmation-modal.component';

describe('RbdSnapshotFormComponent', () => {
  let component: DeleteConfirmationComponent;
  let fixture: ComponentFixture<DeleteConfirmationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        ReactiveFormsModule,
        HttpClientTestingModule,
        ServicesModule,
        ApiModule,
        ToastModule.forRoot()
      ],
      declarations: [ DeleteConfirmationComponent, SubmitButtonComponent ],
      providers: [ BsModalRef, BsModalService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DeleteConfirmationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
