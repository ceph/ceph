import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, Router } from '@angular/router';

import { CephfsAddMirroringPathComponent } from './cephfs-add-mirroring-path.component';

describe('CephfsAddMirroringPathComponent', () => {
  let component: CephfsAddMirroringPathComponent;
  let fixture: ComponentFixture<CephfsAddMirroringPathComponent>;
  let routerNavigateSpy: jest.Mock;

  beforeEach(async () => {
    routerNavigateSpy = jest.fn();

    await TestBed.configureTestingModule({
      declarations: [CephfsAddMirroringPathComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {
              paramMap: convertToParamMap({ fsId: '1', fsName: 'testfs' })
            }
          }
        },
        {
          provide: Router,
          useValue: { navigate: routerNavigateSpy }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(CephfsAddMirroringPathComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(CephfsAddMirroringPathComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read route params on ngOnInit', () => {
    component.ngOnInit();

    expect(component.fsName).toBe('testfs');
    expect(component.fsId).toBe(1);
  });

  it('should define tearsheet metadata', () => {
    expect(component.modalHeaderLabel).toBeDefined();
    expect(component.title).toBeDefined();
    expect(component.steps.length).toBe(3);
    expect(component.steps.map((step) => step.label)).toEqual(['Paths', 'Schedule', 'Review']);
  });

  it('should close modal outlet on submit', () => {
    component.ngOnInit();
    component.onSubmit();

    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  });

  it('should close modal outlet on cancel without reload', () => {
    component.ngOnInit();
    component.onCancel();

    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: undefined }
    );
  });

  it('should decode encoded filesystem name from route params', () => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      declarations: [CephfsAddMirroringPathComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {
              paramMap: convertToParamMap({ fsId: '2', fsName: encodeURIComponent('my fs') })
            }
          }
        },
        {
          provide: Router,
          useValue: { navigate: routerNavigateSpy }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(CephfsAddMirroringPathComponent, {
        set: { template: '' }
      })
      .compileComponents();

    const decodedFixture = TestBed.createComponent(CephfsAddMirroringPathComponent);
    decodedFixture.componentInstance.ngOnInit();

    expect(decodedFixture.componentInstance.fsName).toBe('my fs');
    expect(decodedFixture.componentInstance.fsId).toBe(2);
  });
});
