import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { ComponentsModule } from '~/app/shared/components/components.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RbdNamespaceFormModalComponent } from './rbd-namespace-form-modal.component';

describe('RbdNamespaceFormModalComponent', () => {
  let component: RbdNamespaceFormModalComponent;
  let fixture: ComponentFixture<RbdNamespaceFormModalComponent>;

  configureTestBed({
    imports: [
      ReactiveFormsModule,
      ComponentsModule,
      HttpClientTestingModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    declarations: [RbdNamespaceFormModalComponent],
    providers: [NgbActiveModal, AuthStorageService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdNamespaceFormModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
