import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../../shared/components/components.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
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
    providers: [BsModalRef, BsModalService, AuthStorageService, i18nProviders]
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
