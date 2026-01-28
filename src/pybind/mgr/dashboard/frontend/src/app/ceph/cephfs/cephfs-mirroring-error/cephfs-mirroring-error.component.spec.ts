import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { of } from 'rxjs';

import { CephfsMirroringErrorComponent } from './cephfs-mirroring-error.component';
import { DocService } from '~/app/shared/services/doc.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';

describe('CephfsMirroringErrorComponent', () => {
  let component: CephfsMirroringErrorComponent;
  let fixture: ComponentFixture<CephfsMirroringErrorComponent>;

  const routerMock = {
    events: of({}),
    onSameUrlNavigation: 'reload' as const,
    navigate: jest.fn()
  };

  const docServiceMock = {
    urlGenerator: jest.fn().mockReturnValue('https://docs.ceph.com/en/main/cephfs/')
  };

  const mgrModuleServiceMock = {
    updateModuleState: jest.fn()
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    Object.defineProperty(history, 'state', { value: {}, configurable: true });

    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringErrorComponent],
      imports: [SharedModule, RouterTestingModule],
      providers: [
        { provide: Router, useValue: routerMock },
        { provide: DocService, useValue: docServiceMock },
        { provide: MgrModuleService, useValue: mgrModuleServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringErrorComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should set default header and section when history.state is empty', () => {
    fixture.detectChanges();
    expect(component.header).toBe('Mirroring module is not enabled');
    expect(component.sectionInfo).toBe('CephFS Mirroring');
    expect(component.section).toBe('cephfs-mirroring');
  });

  it('should read header and message from history.state when available', () => {
    Object.defineProperty(history, 'state', {
      value: {
        header: 'Custom header',
        message: 'Custom message',
        section: 'custom-section',
        section_info: 'Custom Section'
      },
      configurable: true
    });
    component.fetchData();
    expect(component.header).toBe('Custom header');
    expect(component.message).toBe('Custom message');
    expect(component.section).toBe('custom-section');
    expect(component.sectionInfo).toBe('Custom Section');
  });

  it('should call docService.urlGenerator with section', () => {
    component.fetchData();
    expect(docServiceMock.urlGenerator).toHaveBeenCalledWith('cephfs-mirroring');
  });

  it('should call mgrModuleService.updateModuleState when enableModule is called', () => {
    fixture.detectChanges();
    component.enableModule();
    expect(mgrModuleServiceMock.updateModuleState).toHaveBeenCalledWith(
      'mirroring',
      false,
      null,
      'File/Mirroring'
    );
  });
});
