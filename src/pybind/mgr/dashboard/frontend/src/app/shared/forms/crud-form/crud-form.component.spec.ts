import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { CrudFormComponent } from './crud-form.component';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

describe('CrudFormComponent', () => {
  let component: CrudFormComponent;
  let fixture: ComponentFixture<CrudFormComponent>;
  let dataGateway: DataGatewayService;
  let taskWrapper: TaskWrapperService;

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule],
    providers: [{ provide: CdDatePipe, useValue: { transform: (d: any) => d } }]
  });

  configureTestBed({
    declarations: [CrudFormComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CrudFormComponent);
    component = fixture.componentInstance;
    dataGateway = TestBed.inject(DataGatewayService);
    taskWrapper = TestBed.inject(TaskWrapperService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('user creation key type', () => {
    const taskInfo = { metadataFields: ['user_entity'], message: 'created' };
    const baseUser = {
      user_entity: 'client.test',
      capabilities: [{ entity: 'mon', cap: 'allow *' }]
    };

    beforeEach(() => {
      component.resource = 'api.cluster.user@1.0';
      component.methodType = 'post';
      component.urlFormName = 'create';
      spyOn(dataGateway, 'submit').and.returnValue(of({}));
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.returnValue(of({}));
    });

    it('should include aes in the create request', async () => {
      await component.submit({ ...baseUser, key_type: 'aes' }, taskInfo);
      expect(dataGateway.submit).toHaveBeenCalledWith(
        'api.cluster.user@1.0',
        jasmine.objectContaining({
          user_entity: 'client.test',
          key_type: 'aes'
        }),
        'post'
      );
    });

    it('should include aes256k in the create request', async () => {
      await component.submit({ ...baseUser, key_type: 'aes256k' }, taskInfo);
      expect(dataGateway.submit).toHaveBeenCalledWith(
        'api.cluster.user@1.0',
        jasmine.objectContaining({
          user_entity: 'client.test',
          key_type: 'aes256k'
        }),
        'post'
      );
    });

    it('should preserve existing user creation payload fields', async () => {
      await component.submit({ ...baseUser, key_type: 'aes' }, taskInfo);
      expect(dataGateway.submit).toHaveBeenCalledWith(
        'api.cluster.user@1.0',
        jasmine.objectContaining({
          user_entity: 'client.test',
          capabilities: [{ entity: 'mon', cap: 'allow *' }]
        }),
        'post'
      );
    });
  });
});
