import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { CrudFormComponent } from './crud-form.component';

describe('CrudFormComponent', () => {
  let component: CrudFormComponent;
  let fixture: ComponentFixture<CrudFormComponent>;
  let dataGatewayService: DataGatewayService;
  let actionLabels: ActionLabelsI18n;

  const formSchema = {
    title: 'Edit User',
    methodType: 'PUT',
    model: {},
    controlSchema: [],
    uiSchema: {},
    taskInfo: { metadataFields: [], message: '' }
  };

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule],
    declarations: [CrudFormComponent],
    providers: [
      { provide: CdDatePipe, useValue: { transform: (d: any) => d } },
      {
        provide: ActivatedRoute,
        useValue: {
          queryParamMap: of(new Map()),
          data: of({ resource: 'cluster/user' }),
          snapshot: { url: [{ path: 'cluster' }, { path: 'user' }, { path: 'edit' }] }
        }
      },
      {
        provide: Router,
        useValue: { url: '/cluster/user/edit' }
      },
      {
        provide: DataGatewayService,
        useValue: {
          form: jasmine.createSpy('form').and.returnValue(of(formSchema))
        }
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CrudFormComponent);
    component = fixture.componentInstance;
    dataGatewayService = TestBed.inject(DataGatewayService);
    actionLabels = TestBed.inject(ActionLabelsI18n);
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should set submit label to Save changes in edit mode', () => {
    fixture.detectChanges();

    expect(dataGatewayService.form).toHaveBeenCalled();
    expect(component.submitAction).toBe('Save changes');
    expect(component.submitAction).toBe(actionLabels.SAVE_CHANGES);
  });

  it('should keep schema title as submit label in create mode', () => {
    const router = TestBed.inject(Router) as any;
    router.url = '/cluster/user/create';
    (dataGatewayService.form as jasmine.Spy).and.returnValue(
      of({ ...formSchema, title: 'Create User', methodType: 'POST' })
    );

    fixture.detectChanges();

    expect(component.submitAction).toBe('Create User');
  });
});
