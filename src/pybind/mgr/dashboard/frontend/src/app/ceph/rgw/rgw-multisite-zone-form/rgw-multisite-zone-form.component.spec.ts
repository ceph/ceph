import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';

import { RgwMultisiteZoneFormComponent } from './rgw-multisite-zone-form.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwMultisiteZoneFormComponent', () => {
  let component: RgwMultisiteZoneFormComponent;
  let fixture: ComponentFixture<RgwMultisiteZoneFormComponent>;

  configureTestBed({
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal],
    declarations: [RgwMultisiteZoneFormComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteZoneFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
