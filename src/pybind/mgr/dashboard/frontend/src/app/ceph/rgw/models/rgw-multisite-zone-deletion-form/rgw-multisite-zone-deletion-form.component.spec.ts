import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwZone } from '../rgw-multisite';

import { RgwMultisiteZoneDeletionFormComponent } from './rgw-multisite-zone-deletion-form.component';

describe('RgwMultisiteZoneDeletionFormComponent', () => {
  let component: RgwMultisiteZoneDeletionFormComponent;
  let fixture: ComponentFixture<RgwMultisiteZoneDeletionFormComponent>;

  configureTestBed({
    declarations: [RgwMultisiteZoneDeletionFormComponent],
    imports: [SharedModule, HttpClientTestingModule, ToastrModule.forRoot(), RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteZoneDeletionFormComponent);
    component = fixture.componentInstance;
    component.zone = new RgwZone();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
