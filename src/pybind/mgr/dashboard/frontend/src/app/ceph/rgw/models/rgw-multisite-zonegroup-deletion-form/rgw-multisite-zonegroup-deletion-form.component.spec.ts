import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwZonegroup } from '../rgw-multisite';

import { RgwMultisiteZonegroupDeletionFormComponent } from './rgw-multisite-zonegroup-deletion-form.component';

describe('RgwMultisiteZonegroupDeletionFormComponent', () => {
  let component: RgwMultisiteZonegroupDeletionFormComponent;
  let fixture: ComponentFixture<RgwMultisiteZonegroupDeletionFormComponent>;

  configureTestBed({
    declarations: [RgwMultisiteZonegroupDeletionFormComponent],
    imports: [SharedModule, HttpClientTestingModule, ToastrModule.forRoot(), RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteZonegroupDeletionFormComponent);
    component = fixture.componentInstance;
    component.zonegroup = new RgwZonegroup();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
