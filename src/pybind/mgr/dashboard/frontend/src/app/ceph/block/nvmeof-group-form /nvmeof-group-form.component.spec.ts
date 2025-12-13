import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofGroupFormComponent } from './nvmeof-group-form.component';
import { GridModule, InputModule, SelectModule } from 'carbon-components-angular';

describe('NvmeofSubsystemsFormComponent', () => {
  let component: NvmeofGroupFormComponent;
  let fixture: ComponentFixture<NvmeofGroupFormComponent>;
  let form: CdFormGroup;
  const mockTimestamp = 1720693470789;
  const mockGroupName = 'default';

  beforeEach(async () => {
    spyOn(Date, 'now').and.returnValue(mockTimestamp);
    await TestBed.configureTestingModule({
      declarations: [NvmeofGroupFormComponent],
      providers: [NgbActiveModal],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        GridModule,
        InputModule,
        SelectModule,
        ToastrModule.forRoot()
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGroupFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    form = component.groupForm;
    fixture.detectChanges();
    component.group = mockGroupName;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
  it('should create form with empty fields', () => {
    expect(form.controls.groupName.value).toBeNull();
    expect(form.controls.pool.value).toBeNull();
  });
});
