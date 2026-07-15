import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of } from 'rxjs';

import { CephfsMirroringEntityComponent } from './cephfs-mirroring-entity.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ClusterService } from '~/app/shared/api/cluster.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

describe('CephfsMirroringEntityComponent', () => {
  let component: CephfsMirroringEntityComponent;
  let fixture: ComponentFixture<CephfsMirroringEntityComponent>;

  let clusterServiceMock: any;
  let cephfsServiceMock: any;
  let taskWrapperServiceMock: any;

  beforeEach(async () => {
    clusterServiceMock = {
      listUser: jest.fn(),
      createUser: jest.fn()
    };

    cephfsServiceMock = {
      setAuth: jest.fn()
    };

    taskWrapperServiceMock = {
      wrapTaskAroundCall: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringEntityComponent],
      imports: [ReactiveFormsModule],
      providers: [
        CdFormBuilder,
        { provide: ClusterService, useValue: clusterServiceMock },
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperServiceMock }
      ]
    })
      .overrideComponent(CephfsMirroringEntityComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringEntityComponent);
    component = fixture.componentInstance;

    component.selectedFilesystem = { name: 'myfs' } as any;

    clusterServiceMock.listUser.mockReturnValue(of([]));

    fixture.detectChanges();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create component', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize form with required + noClientPrefix validator', () => {
    const control = component.entityForm.get('user_entity');

    control?.setValue('');
    expect(control?.valid).toBeFalsy();

    control?.setValue('client.test');
    expect(control?.errors?.['forbiddenClientPrefix']).toBeTruthy();

    control?.setValue('validuser');
    expect(control?.valid).toBeTruthy();
  });

  it('should filter entities based on fsname', fakeAsync(() => {
    clusterServiceMock.listUser.mockReturnValue(
      of([
        {
          entity: 'client.valid',
          caps: { mds: 'allow rw fsname=myfs' }
        },
        {
          entity: 'client.invalid',
          caps: { mds: 'allow rw fsname=otherfs' }
        },
        {
          entity: 'osd.something',
          caps: { mds: 'allow rw fsname=myfs' }
        }
      ])
    );

    component.loadEntities();
    tick();

    component.entities$.subscribe((rows) => {
      expect(rows.length).toBe(1);
      expect(rows[0].entity).toBe('client.valid');
      expect(rows[0].mdsCaps).toContain('fsname=myfs');
    });
  }));

  it('should not submit if form invalid', () => {
    component.entityForm.get('user_entity')?.setValue('');

    component.submitAction();

    expect(clusterServiceMock.createUser).not.toHaveBeenCalled();
    expect(component.isSubmitting).toBeFalsy();
  });

  it('should create entity and set auth successfully', fakeAsync(() => {
    component.entityForm.get('user_entity')?.setValue('newuser');

    clusterServiceMock.createUser.mockReturnValue(of({}));
    taskWrapperServiceMock.wrapTaskAroundCall.mockReturnValue(of({}));
    cephfsServiceMock.setAuth.mockReturnValue(of({}));

    const emitSpy = jest.spyOn(component.entitySelected, 'emit');

    component.submitAction();
    tick();

    expect(clusterServiceMock.createUser).toHaveBeenCalledWith(
      expect.objectContaining({
        user_entity: 'client.newuser'
      })
    );

    expect(cephfsServiceMock.setAuth).toHaveBeenCalledWith('myfs', 'newuser', ['/', 'rwps'], false);

    expect(emitSpy).toHaveBeenCalledWith('client.newuser');
    expect(component.isSubmitting).toBeFalsy();
    expect(component.isCreatingNewEntity).toBeFalsy();
  }));

  it('should emit selected entity on updateSelection', () => {
    const selection = new CdTableSelection();
    selection.selected = [{ entity: 'client.test' }];

    const emitSpy = jest.spyOn(component.entitySelected, 'emit');

    component.updateSelection(selection);

    expect(emitSpy).toHaveBeenCalledWith('client.test');
  });

  it('should emit null when selection empty', () => {
    const selection = new CdTableSelection();
    selection.selected = [];

    const emitSpy = jest.spyOn(component.entitySelected, 'emit');

    component.updateSelection(selection);

    expect(emitSpy).toHaveBeenCalledWith(null);
  });

  it('should toggle create/existing states correctly', () => {
    component.onExistingEntitySelected();
    expect(component.isCreatingNewEntity).toBeFalsy();

    component.onCreateEntitySelected();
    expect(component.isCreatingNewEntity).toBeTruthy();
  });

  it('should dismiss warnings correctly', () => {
    component.onDismissCreateRequirementsWarning();
    expect(component.showCreateRequirementsWarning).toBeFalsy();

    component.onDismissCreateCapabilitiesInfo();
    expect(component.showCreateCapabilitiesInfo).toBeFalsy();

    component.onDismissSelectRequirementsWarning();
    expect(component.showSelectRequirementsWarning).toBeFalsy();

    component.onDismissSelectEntityInfo();
    expect(component.showSelectEntityInfo).toBeFalsy();
  });
});
