import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiClusterFormComponent } from './multi-cluster-form.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { NotificationService } from '~/app/shared/services/notification.service';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { CheckboxModule, GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('MultiClusterFormComponent', () => {
  let component: MultiClusterFormComponent;
  let fixture: ComponentFixture<MultiClusterFormComponent>;

  configureTestBed({
    imports: [
      SharedModule,
      CommonModule,
      FormsModule,
      CheckboxModule,
      GridModule,
      ReactiveFormsModule,
      InputModule,
      SelectModule,
      RouterTestingModule,
      HttpClientTestingModule
    ],
    declarations: [MultiClusterFormComponent],
    providers: [NgbActiveModal, NotificationService, CdDatePipe, DatePipe]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(MultiClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
