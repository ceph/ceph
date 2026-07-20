import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteSyncPipeModalComponent } from './rgw-multisite-sync-pipe-modal.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';
import { USER } from '~/app/shared/constants/app.constants';
import { FlowType } from '../models/rgw-multisite';
import { ComboBoxModule } from 'carbon-components-angular';

class MultisiteServiceMock {
  createEditSyncPipe = jest.fn().mockReturnValue(of(null));
}

describe('RgwMultisiteSyncPipeModalComponent', () => {
  let component: RgwMultisiteSyncPipeModalComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPipeModalComponent>;
  let multisiteServiceMock: MultisiteServiceMock;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPipeModalComponent],
      imports: [
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        PipesModule,
        ReactiveFormsModule,
        CommonModule,
        ComboBoxModule
      ],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA],
      providers: [
        NgbActiveModal,
        { provide: RgwMultisiteService, useClass: MultisiteServiceMock },
        { provide: 'groupType', useValue: FlowType.symmetrical },
        { provide: 'groupExpandedRow', useValue: { groupName: 'new', bucket: 'bucket1' } },
        {
          provide: 'pipeSelectedRow',
          useValue: {
            source: { zones: ['zone1-zg1-realm1'], bucket: 'bucket1' },
            dest: { zones: ['zone2-zg1-realm1'], bucket: 'bucket1' },
            id: 'pipe1',
            params: { user: 'dashboard', mode: USER }
          }
        },
        { provide: 'action', useValue: 'create' }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPipeModalComponent);
    multisiteServiceMock = (TestBed.inject(RgwMultisiteService) as unknown) as MultisiteServiceMock;
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should replace `*` with `All Zones (*)`', () => {
    let zones = ['*', 'zone1-zg1-realm1', 'zone2-zg1-realm1'];
    let mockReturnVal = ['All Zones (*)', 'zone1-zg1-realm1', 'zone2-zg1-realm1'];
    const spy = jest.spyOn(component, 'replaceAsteriskWithString').mockReturnValue(mockReturnVal);
    const res = component.replaceAsteriskWithString(zones);
    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledWith(zones);
    expect(res).toEqual(mockReturnVal);
  });

  it('should replace `All Zones (*)` with `*`', () => {
    let zones = ['All Zones (*)', 'zone1-zg1-realm1', 'zone2-zg1-realm1'];
    let mockReturnVal = ['*', 'zone1-zg1-realm1', 'zone2-zg1-realm1'];
    const spy = jest.spyOn(component, 'replaceWithAsterisk').mockReturnValue(mockReturnVal);
    const res = component.replaceWithAsterisk(zones);
    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledWith(zones);
    expect(res).toEqual(mockReturnVal);
  });

  it('should assign zone value', () => {
    let zonesAdded: string[] = [];
    let selectedZone = ['zone2-zg1-realm1'];
    const spy = jest.spyOn(component, 'assignZoneValue').mockReturnValue(selectedZone);
    const res = component.assignZoneValue(zonesAdded, selectedZone);
    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledWith(zonesAdded, selectedZone);
    expect(res).toEqual(selectedZone);
  });

  it('should call createEditSyncPipe for creating/editing sync pipe', () => {
    component.editing = false;
    component.ngOnInit();
    component.pipeForm.patchValue({
      pipe_id: 'pipe1',
      group_id: 'new',
      source_bucket: '',
      source_zones: ['zone1-zg1-realm1'],
      destination_bucket: '',
      destination_zones: ['zone2-zg1-realm1']
    });
    component.sourceZones = [
      { name: 'zone1-zg1-realm1', content: 'zone1-zg1-realm1', selected: true }
    ];
    component.destZones = [
      { name: 'zone2-zg1-realm1', content: 'zone2-zg1-realm1', selected: true }
    ];
    const spy = jest.spyOn(component, 'submit');
    const putDataSpy = jest.spyOn(multisiteServiceMock, 'createEditSyncPipe');
    component.submit();
    expect(spy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalledWith({
      ...component.pipeForm.getRawValue(),
      source_zones: { added: ['zone1-zg1-realm1'], removed: [] },
      destination_zones: { added: ['zone2-zg1-realm1'], removed: [] },
      mode: '',
      user: ''
    });
  });

  it('should pass "user" and "mode" while creating/editing pipe', () => {
    component.editing = true;
    component.pipeForm.patchValue({
      pipe_id: 'pipe1',
      group_id: 's3-bucket-replication:enabled',
      source_bucket: '',
      source_zones: ['zone1-zg1-realm1'],
      destination_bucket: '',
      destination_zones: ['zone2-zg1-realm1']
    });
    component.pipeSelectedRow = {
      dest: { bucket: '*', zones: ['zone2-zg1-realm1'] },
      id: 'pipi1',
      params: {
        mode: USER,
        user: 'dashboard'
      },
      source: { bucket: '*', zones: ['zone1-zg1-realm1'] }
    };

    component.sourceZones = [
      { name: 'zone1-zg1-realm1', content: 'zone1-zg1-realm1', selected: true }
    ];
    component.destZones = [
      { name: 'zone2-zg1-realm1', content: 'zone2-zg1-realm1', selected: true }
    ];
    const spy = jest.spyOn(component, 'submit');
    const putDataSpy = jest.spyOn(multisiteServiceMock, 'createEditSyncPipe');
    component.submit();
    expect(spy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalledWith({
      ...component.pipeForm.getRawValue(),
      source_zones: { added: ['zone1-zg1-realm1'], removed: [] },
      destination_zones: { added: ['zone2-zg1-realm1'], removed: [] },
      mode: USER,
      user: 'dashboard'
    });
  });
});
