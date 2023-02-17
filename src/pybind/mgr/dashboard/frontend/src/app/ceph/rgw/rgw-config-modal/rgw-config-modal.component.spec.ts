import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwConfigModalComponent } from './rgw-config-modal.component';

describe('RgwConfigModalComponent', () => {
  let component: RgwConfigModalComponent;
  let fixture: ComponentFixture<RgwConfigModalComponent>;

  configureTestBed({
    declarations: [RgwConfigModalComponent],
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwConfigModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
