import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwMultisiteSyncFlowModalComponent } from './rgw-multisite-sync-flow-modal.component';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { of } from 'rxjs';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';
import { ComboBoxModule, ModalModule, SelectModule } from 'carbon-components-angular';

enum FlowType {
  symmetrical = 'symmetrical',
  directional = 'directional'
}

class MultisiteServiceMock {
  createEditSyncFlow = jest.fn().mockReturnValue(of(null));
}

describe('RgwMultisiteSyncFlowModalComponent', () => {
  let component: RgwMultisiteSyncFlowModalComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncFlowModalComponent>;
  let multisiteServiceMock: MultisiteServiceMock;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncFlowModalComponent],
      imports: [
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        PipesModule,
        ReactiveFormsModule,
        CommonModule,
        SelectModule,
        ModalModule,
        ComboBoxModule
      ],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA],
      providers: [
        { provide: RgwMultisiteService, useClass: MultisiteServiceMock },
        { provide: 'groupType', useValue: FlowType.symmetrical },
        { provide: 'groupExpandedRow', useValue: { groupName: 'new', bucket: 'bucket1' } },
        {
          provide: 'flowSelectedRow',
          useValue: { id: 'symmetrical', zones: ['zone1-zg1-realm1'] }
        },
        { provide: 'action', useValue: 'create' }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncFlowModalComponent);
    multisiteServiceMock = (TestBed.inject(RgwMultisiteService) as unknown) as MultisiteServiceMock;
    component = fixture.componentInstance;
    component.groupType = FlowType.symmetrical;
    component.groupExpandedRow = { groupName: 'new', bucket: 'bucket1' };
    component.flowSelectedRow = { id: 'symmetrical', zones: ['zone1-zg1-realm1'] };
    component.action = 'create';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
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

  it('should call createEditSyncFlow for creating/editing symmetrical sync flow', () => {
    component.editing = false;
    component.ngOnInit();
    component.currentFormGroupContext.patchValue({
      flow_id: 'symmetrical',
      group_id: 'new',
      zones: ['zone1-zg1-realm1']
    });
    const spy = jest.spyOn(component, 'submit');
    const putDataSpy = jest
      .spyOn(multisiteServiceMock, 'createEditSyncFlow')
      .mockReturnValue(of(null));
    component.submit();
    expect(spy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalledWith({
      ...component.currentFormGroupContext.getRawValue(),
      zones: { added: ['zone1-zg1-realm1'], removed: [] }
    });
  });

  it('should call createEditSyncFlow for creating/editing directional sync flow', () => {
    component.editing = false;
    component.groupType = FlowType.directional;
    component.ngOnInit();
    component.currentFormGroupContext.patchValue({
      flow_id: 'directional',
      group_id: 'new',
      source_zone: ['zone1-zg1-realm1'],
      destination_zone: ['zone2-zg1-realm1']
    });
    const spy = jest.spyOn(component, 'submit');
    const putDataSpy = jest
      .spyOn(multisiteServiceMock, 'createEditSyncFlow')
      .mockReturnValue(of(null));
    component.submit();
    expect(spy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalled();
    expect(putDataSpy).toHaveBeenCalledWith({
      ...component.currentFormGroupContext.getRawValue(),
      zones: { added: [], removed: [] }
    });
  });
});
