import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSystemUserComponent } from './rgw-system-user.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwSystemUserComponent', () => {
  let component: RgwSystemUserComponent;
  let fixture: ComponentFixture<RgwSystemUserComponent>;

  configureTestBed({
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ],
    declarations: [RgwSystemUserComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwSystemUserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
