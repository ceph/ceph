import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { expect as jestExpect } from '@jest/globals';
import { of } from 'rxjs';

import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsVolumeFormComponent } from '../cephfs-form/cephfs-form.component';
import { CephfsListComponent } from './cephfs-list.component';
import { CephfsActionService } from '~/app/shared/services/cephfs-action.service';

@Component({ selector: 'cd-cephfs-tabs', template: '', standalone: false })
class CephfsTabsStubComponent {
  @Input()
  selection: CdTableSelection;
}

describe('CephfsListComponent', () => {
  let component: CephfsListComponent;
  let fixture: ComponentFixture<CephfsListComponent>;
  const cephfsActionService = {
    getMonAllowPoolDelete: () => of(false),
    getDeleteDisableDesc: () => true,
    showAttachInfo: () => undefined,
    removeVolume: () => undefined,
    authorize: () => undefined
  };

  configureTestBed({
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, RouterTestingModule],
    declarations: [CephfsListComponent, CephfsTabsStubComponent, CephfsVolumeFormComponent],
    providers: [
      {
        provide: CephfsActionService,
        useValue: cephfsActionService
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    jestExpect(component).toBeTruthy();
  });

  describe('remove action', () => {
    it('should delegate volume removal to the shared cephfs action service', () => {
      spyOn(cephfsActionService, 'removeVolume').and.stub();
      component.selection.selected = [{ mdsmap: { fs_name: 'somevolumeName' } }];

      const removeAction = component.tableActions.find(
        (action) => action.name === component.actionLabels.REMOVE
      );
      removeAction.click();

      jestExpect(cephfsActionService.removeVolume).toHaveBeenCalledWith(
        'somevolumeName',
        component.deleteTpl
      );
    });
  });
});
