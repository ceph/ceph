import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { TitleCasePipe } from '@angular/common';
import { of, throwError } from 'rxjs';

import { RgwMultisiteSyncPolicyResourcePageComponent } from './rgw-multisite-sync-policy-resource-page.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { FlowType } from '~/app/ceph/rgw/models/rgw-multisite';

import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

describe('RgwMultisiteSyncPolicyResourcePageComponent', () => {
  let component: RgwMultisiteSyncPolicyResourcePageComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyResourcePageComponent>;
  let rgwMultisiteServiceMock: any;
  let modalCdsServiceMock: any;
  let taskWrapperServiceMock: any;

  beforeEach(async () => {
    rgwMultisiteServiceMock = {
      getSyncPolicyGroup: jest.fn().mockReturnValue(
        of({
          status: 'allowed',
          zonegroup: 'zg1',
          data_flow: {
            symmetrical: [{ id: 'sym-flow-1' }],
            directional: [{ id: 'dir-flow-1' }]
          },
          pipes: [{ id: 'pipe-1' }]
        })
      ),
      removeSyncFlow: jest.fn().mockReturnValue(of({})),
      removeSyncPipe: jest.fn().mockReturnValue(of({}))
    };

    modalCdsServiceMock = {
      show: jest.fn().mockReturnValue({
        result: Promise.resolve(NotificationType.success)
      })
    };

    taskWrapperServiceMock = {
      wrapTaskAroundCall: jest.fn().mockImplementation((args) => args.call)
    };

    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyResourcePageComponent],
      providers: [
        TitleCasePipe,
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'overview' }),
            queryParamMap: of({ get: (key: string) => (key === 'bucketName' ? '' : null) }),
            parent: {
              paramMap: of({
                get: (key: string) => (key === 'groupName' ? 'sync-group-a' : null)
              }),
              queryParamMap: of({ get: () => null })
            },
            paramMap: of({ get: () => null })
          }
        },
        {
          provide: ActionLabelsI18n,
          useValue: {
            CREATE: 'Create',
            EDIT: 'Edit',
            DELETE: 'Delete'
          }
        },
        { provide: RgwMultisiteService, useValue: rgwMultisiteServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperServiceMock },
        { provide: ModalCdsService, useValue: modalCdsServiceMock },
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: () => ({
              rgw: { read: true, create: true, update: true, delete: true }
            })
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges(); // Triggers ngOnInit and initial loadData()
  });

  it('should create and populate overview fields on initialization', () => {
    expect(component).toBeTruthy();
    expect(component.groupName).toBe('sync-group-a');
    expect(component.hasPolicyGroup).toBe(true);
    expect(component.notFound).toBe(false);

    // Verify parsed data
    expect(component.symmetricalFlowData.length).toBe(1);
    expect(component.directionalFlowData.length).toBe(1);
    expect(component.pipeData.length).toBe(1);

    // Verify Overview fields
    expect(component.overviewFields.length).toBe(7);
    expect(component.overviewFields[0].value).toBe('sync-group-a'); // Group Name
    expect(component.overviewFields[1].value).toBe('Allowed'); // Status capitalized by TitleCasePipe
  });

  it('should handle missing groupName during loadData', () => {
    component.groupName = '';
    component.loadData();

    expect(component.notFound).toBe(true);
    expect(component.hasPolicyGroup).toBe(false);
    expect(component.overviewFields).toEqual([]);
    expect(component.symmetricalFlowData).toEqual([]);
  });

  it('should handle API errors during loadData gracefully', () => {
    rgwMultisiteServiceMock.getSyncPolicyGroup.mockReturnValueOnce(
      throwError(() => new Error('API failure'))
    );
    component.loadData();

    expect(component.notFound).toBe(true);
    expect(component.hasPolicyGroup).toBe(false);
    expect(component.overviewFields).toEqual([]);
  });

  it('should show delete confirmation and process flow deletion', () => {
    // Mock the table selection
    component.symFlowSelection = {
      hasSelection: true,
      hasSingleSelection: true,
      hasMultiSelection: false,
      first: () => ({ id: 'sym-flow-1' }),
      selected: [{ id: 'sym-flow-1' }]
    } as any;

    component.deleteFlow(FlowType.symmetrical);

    expect(modalCdsServiceMock.show).toHaveBeenCalledWith(
      DeleteConfirmationModalComponent,
      expect.objectContaining({
        itemDescription: 'Flow',
        itemNames: ['sym-flow-1']
      })
    );

    // Extract the submitActionObservable and execute it
    const modalArgs = modalCdsServiceMock.show.mock.calls[0][1];
    const obs$ = modalArgs.submitActionObservable();

    obs$.subscribe();

    expect(rgwMultisiteServiceMock.removeSyncFlow).toHaveBeenCalledWith(
      'sym-flow-1',
      FlowType.symmetrical,
      'sync-group-a',
      undefined // bucketName is not set in this context
    );
  });

  it('should show delete confirmation and process multiple pipe deletions', () => {
    // Mock multiple table selection
    component.pipeSelection = {
      hasSelection: true,
      hasSingleSelection: false,
      hasMultiSelection: true,
      first: () => ({ id: 'pipe-1' }),
      selected: [{ id: 'pipe-1' }, { id: 'pipe-2' }]
    } as any;

    component.deletePipe();

    expect(modalCdsServiceMock.show).toHaveBeenCalledWith(
      DeleteConfirmationModalComponent,
      expect.objectContaining({
        itemDescription: 'Pipes',
        itemNames: ['pipe-1', 'pipe-2']
      })
    );

    // Extract the submitActionObservable and execute it
    const modalArgs = modalCdsServiceMock.show.mock.calls[0][1];
    const obs$ = modalArgs.submitActionObservable();

    obs$.subscribe();

    expect(rgwMultisiteServiceMock.removeSyncPipe).toHaveBeenCalledTimes(2);
    expect(rgwMultisiteServiceMock.removeSyncPipe).toHaveBeenCalledWith(
      'pipe-1',
      'sync-group-a',
      undefined
    );
    expect(rgwMultisiteServiceMock.removeSyncPipe).toHaveBeenCalledWith(
      'pipe-2',
      'sync-group-a',
      undefined
    );
  });

  it('should update correct selection object based on FlowType', () => {
    const mockSelection = { selected: [{ id: 'test' }] } as any;

    component.updateSelection(mockSelection, FlowType.directional);
    expect(component.dirFlowSelection).toBe(mockSelection);

    component.updateSelection(mockSelection, FlowType.symmetrical);
    expect(component.symFlowSelection).toBe(mockSelection);
  });
});

describe('RgwMultisiteSyncPolicyResourcePageComponent', () => {
  let component: RgwMultisiteSyncPolicyResourcePageComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyResourcePageComponent>;
  let rgwMultisiteServiceMock: any;
  let modalCdsServiceMock: any;
  let taskWrapperServiceMock: any;

  beforeEach(async () => {
    rgwMultisiteServiceMock = {
      getSyncPolicyGroup: jest.fn().mockReturnValue(
        of({
          status: 'allowed',
          zonegroup: 'zg1',
          data_flow: {
            symmetrical: [{ id: 'sym-flow-1' }],
            directional: [{ id: 'dir-flow-1' }]
          },
          pipes: [{ id: 'pipe-1' }]
        })
      ),
      removeSyncFlow: jest.fn().mockReturnValue(of({})),
      removeSyncPipe: jest.fn().mockReturnValue(of({}))
    };

    modalCdsServiceMock = {
      show: jest.fn().mockReturnValue({
        result: Promise.resolve(NotificationType.success)
      })
    };

    taskWrapperServiceMock = {
      wrapTaskAroundCall: jest.fn().mockImplementation((args) => args.call)
    };

    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyResourcePageComponent],
      providers: [
        TitleCasePipe,
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'overview' }),
            queryParamMap: of({ get: (key: string) => (key === 'bucketName' ? '' : null) }),
            parent: {
              paramMap: of({
                get: (key: string) => (key === 'groupName' ? 'sync-group-a' : null)
              }),
              queryParamMap: of({ get: () => null })
            },
            paramMap: of({ get: () => null })
          }
        },
        {
          provide: ActionLabelsI18n,
          useValue: {
            CREATE: 'Create',
            EDIT: 'Edit',
            DELETE: 'Delete'
          }
        },
        { provide: RgwMultisiteService, useValue: rgwMultisiteServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperServiceMock },
        { provide: ModalCdsService, useValue: modalCdsServiceMock },
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: () => ({
              rgw: { read: true, create: true, update: true, delete: true }
            })
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges(); // Triggers ngOnInit and initial loadData()
  });

  it('should create and populate overview fields on initialization', () => {
    expect(component).toBeTruthy();
    expect(component.groupName).toBe('sync-group-a');
    expect(component.hasPolicyGroup).toBe(true);
    expect(component.notFound).toBe(false);

    // Verify parsed data
    expect(component.symmetricalFlowData.length).toBe(1);
    expect(component.directionalFlowData.length).toBe(1);
    expect(component.pipeData.length).toBe(1);

    // Verify Overview fields
    expect(component.overviewFields.length).toBe(7);
    expect(component.overviewFields[0].value).toBe('sync-group-a'); // Group Name
    expect(component.overviewFields[1].value).toBe('Allowed'); // Status capitalized by TitleCasePipe
  });

  it('should handle missing groupName during loadData', () => {
    component.groupName = '';
    component.loadData();

    expect(component.notFound).toBe(true);
    expect(component.hasPolicyGroup).toBe(false);
    expect(component.overviewFields).toEqual([]);
    expect(component.symmetricalFlowData).toEqual([]);
  });

  it('should handle API errors during loadData gracefully', () => {
    rgwMultisiteServiceMock.getSyncPolicyGroup.mockReturnValueOnce(
      throwError(() => new Error('API failure'))
    );
    component.loadData();

    expect(component.notFound).toBe(true);
    expect(component.hasPolicyGroup).toBe(false);
    expect(component.overviewFields).toEqual([]);
  });

  it('should show delete confirmation and process flow deletion', () => {
    // Mock the table selection
    component.symFlowSelection = {
      hasSelection: true,
      hasSingleSelection: true,
      hasMultiSelection: false,
      first: () => ({ id: 'sym-flow-1' }),
      selected: [{ id: 'sym-flow-1' }]
    } as any;

    component.deleteFlow(FlowType.symmetrical);

    expect(modalCdsServiceMock.show).toHaveBeenCalledWith(
      DeleteConfirmationModalComponent,
      expect.objectContaining({
        itemDescription: 'Flow',
        itemNames: ['sym-flow-1']
      })
    );

    // Extract the submitActionObservable and execute it
    const modalArgs = modalCdsServiceMock.show.mock.calls[0][1];
    const obs$ = modalArgs.submitActionObservable();

    obs$.subscribe();

    expect(rgwMultisiteServiceMock.removeSyncFlow).toHaveBeenCalledWith(
      'sym-flow-1',
      FlowType.symmetrical,
      'sync-group-a',
      undefined // bucketName is not set in this context
    );
  });

  it('should show delete confirmation and process multiple pipe deletions', () => {
    // Mock multiple table selection
    component.pipeSelection = {
      hasSelection: true,
      hasSingleSelection: false,
      hasMultiSelection: true,
      first: () => ({ id: 'pipe-1' }),
      selected: [{ id: 'pipe-1' }, { id: 'pipe-2' }]
    } as any;

    component.deletePipe();

    expect(modalCdsServiceMock.show).toHaveBeenCalledWith(
      DeleteConfirmationModalComponent,
      expect.objectContaining({
        itemDescription: 'Pipes',
        itemNames: ['pipe-1', 'pipe-2']
      })
    );

    // Extract the submitActionObservable and execute it
    const modalArgs = modalCdsServiceMock.show.mock.calls[0][1];
    const obs$ = modalArgs.submitActionObservable();

    obs$.subscribe();

    expect(rgwMultisiteServiceMock.removeSyncPipe).toHaveBeenCalledTimes(2);
    expect(rgwMultisiteServiceMock.removeSyncPipe).toHaveBeenCalledWith(
      'pipe-1',
      'sync-group-a',
      undefined
    );
    expect(rgwMultisiteServiceMock.removeSyncPipe).toHaveBeenCalledWith(
      'pipe-2',
      'sync-group-a',
      undefined
    );
  });

  it('should update correct selection object based on FlowType', () => {
    const mockSelection = { selected: [{ id: 'test' }] } as any;

    component.updateSelection(mockSelection, FlowType.directional);
    expect(component.dirFlowSelection).toBe(mockSelection);

    component.updateSelection(mockSelection, FlowType.symmetrical);
    expect(component.symFlowSelection).toBe(mockSelection);
  });
});
