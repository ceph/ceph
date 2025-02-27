import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteMigrateComponent } from './rgw-multisite-migrate.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwMultisiteMigrateComponent', () => {
  let component: RgwMultisiteMigrateComponent;
  let fixture: ComponentFixture<RgwMultisiteMigrateComponent>;

  configureTestBed({
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ],
    declarations: [RgwMultisiteMigrateComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteMigrateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
