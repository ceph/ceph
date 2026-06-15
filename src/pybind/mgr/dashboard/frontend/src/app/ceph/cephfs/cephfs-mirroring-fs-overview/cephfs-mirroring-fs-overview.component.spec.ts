import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of } from 'rxjs';

import { CephfsMirroringFsOverviewComponent } from './cephfs-mirroring-fs-overview.component';

describe('CephfsMirroringFsOverviewComponent', () => {
  let component: CephfsMirroringFsOverviewComponent;
  let fixture: ComponentFixture<CephfsMirroringFsOverviewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringFsOverviewComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              paramMap: of(convertToParamMap({ fsName: 'myfs' }))
            }
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringFsOverviewComponent);
    component = fixture.componentInstance;

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read fsName from parent route params', () => {
    expect(component.fsName).toBe('myfs');
  });
});
