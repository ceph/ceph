import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { CephfsMirroringFsTabsComponent } from './cephfs-mirroring-fs-tabs.component';

describe('CephfsMirroringFsTabsComponent', () => {
  let component: CephfsMirroringFsTabsComponent;
  let fixture: ComponentFixture<CephfsMirroringFsTabsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RouterTestingModule],
      declarations: [CephfsMirroringFsTabsComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ fsName: 'testfs' }))
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringFsTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read fsName from route params', () => {
    expect(component.fsName).toBe('testfs');
    expect(component.displayFsName).toBe('testfs');
  });
});
