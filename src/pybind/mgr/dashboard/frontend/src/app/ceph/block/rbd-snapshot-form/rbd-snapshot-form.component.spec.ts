import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { ApiModule } from '../../../shared/api/api.module';
import { ComponentsModule } from '../../../shared/components/components.module';
import { PipesModule } from '../../../shared/pipes/pipes.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { RbdSnapshotFormComponent } from './rbd-snapshot-form.component';

describe('RbdSnapshotFormComponent', () => {
  let component: RbdSnapshotFormComponent;
  let fixture: ComponentFixture<RbdSnapshotFormComponent>;

  configureTestBed({
    imports: [
      ReactiveFormsModule,
      ComponentsModule,
      PipesModule,
      HttpClientTestingModule,
      ApiModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    declarations: [RbdSnapshotFormComponent],
    providers: [BsModalRef, BsModalService, AuthStorageService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdSnapshotFormComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show "Create" text', () => {
    fixture.detectChanges();

    const header = fixture.debugElement.nativeElement.querySelector('h4');
    expect(header.textContent).toBe('Create RBD Snapshot');

    const button = fixture.debugElement.nativeElement.querySelector('cd-submit-button');
    expect(button.textContent).toBe('Create RBD Snapshot');
  });

  it('should show "Rename" text', () => {
    component.setEditing();

    fixture.detectChanges();

    const header = fixture.debugElement.nativeElement.querySelector('h4');
    expect(header.textContent).toBe('Rename RBD Snapshot');

    const button = fixture.debugElement.nativeElement.querySelector('cd-submit-button');
    expect(button.textContent).toBe('Rename RBD Snapshot');
  });
});
