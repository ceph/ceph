import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofListenersFormComponent } from './nvmeof-listeners-form.component';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('NvmeofListenersFormComponent', () => {
  let component: NvmeofListenersFormComponent;
  let fixture: ComponentFixture<NvmeofListenersFormComponent>;

  configureTestBed({
    declarations: [NvmeofListenersFormComponent],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: {
          params: of({ subsystem_nqn: 'nqn.2001-07.com.ceph:1' }),
          queryParams: of({ group: 'group1' }),
          snapshot: { params: { subsystem_nqn: 'nqn.2001-07.com.ceph:1' } },
          parent: {
            snapshot: { params: { subsystem_nqn: 'nqn.2001-07.com.ceph:1' } },
            params: of({ subsystem_nqn: 'nqn.2001-07.com.ceph:1' })
          }
        }
      }
    ],
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
    schemas: [CUSTOM_ELEMENTS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofListenersFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
