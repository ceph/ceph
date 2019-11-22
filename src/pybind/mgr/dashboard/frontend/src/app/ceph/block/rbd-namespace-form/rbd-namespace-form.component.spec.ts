import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { ApiModule } from '../../../shared/api/api.module';
import { ComponentsModule } from '../../../shared/components/components.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { RbdNamespaceFormComponent } from './rbd-namespace-form.component';

describe('RbdNamespaceFormComponent', () => {
  let component: RbdNamespaceFormComponent;
  let fixture: ComponentFixture<RbdNamespaceFormComponent>;

  configureTestBed({
    imports: [
      ReactiveFormsModule,
      ComponentsModule,
      HttpClientTestingModule,
      ApiModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    declarations: [RbdNamespaceFormComponent],
    providers: [BsModalRef, BsModalService, AuthStorageService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdNamespaceFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
