import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TreeModule } from 'angular-tree-component';
import { of } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { HealthService } from '../../../shared/api/health.service';
import { SharedModule } from '../../../shared/shared.module';
import { CrushmapComponent } from './crushmap.component';

describe('CrushmapComponent', () => {
  let component: CrushmapComponent;
  let fixture: ComponentFixture<CrushmapComponent>;
  let debugElement: DebugElement;
  configureTestBed({
    imports: [HttpClientTestingModule, TreeModule.forRoot(), SharedModule],
    declarations: [CrushmapComponent],
    providers: [HealthService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CrushmapComponent);
    component = fixture.componentInstance;
    debugElement = fixture.debugElement;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display right title', () => {
    fixture.detectChanges();
    const span = debugElement.nativeElement.querySelector('.card-header');
    expect(span.textContent).toBe('CRUSH map viewer');
  });

  describe('test tree', () => {
    let healthService: HealthService;
    const prepareGetHealth = (nodes: object[]) => {
      spyOn(healthService, 'getFullHealth').and.returnValue(
        of({ osd_map: { tree: { nodes: nodes } } })
      );
      fixture.detectChanges();
    };

    beforeEach(() => {
      healthService = debugElement.injector.get(HealthService);
    });

    it('should display "No nodes!" if ceph tree nodes is empty array', () => {
      prepareGetHealth([]);
      expect(healthService.getFullHealth).toHaveBeenCalled();
      expect(component.nodes[0].name).toEqual('No nodes!');
    });

    describe('nodes not empty', () => {
      beforeEach(() => {
        prepareGetHealth([
          { children: [-2], type: 'root', name: 'default', id: -1 },
          { children: [1, 0, 2], type: 'host', name: 'my-host', id: -2 },
          { status: 'up', type: 'osd', name: 'osd.0', id: 0 },
          { status: 'down', type: 'osd', name: 'osd.1', id: 1 },
          { status: 'up', type: 'osd', name: 'osd.2', id: 2 },
          { children: [-4], type: 'root', name: 'default-2', id: -3 },
          { children: [4], type: 'host', name: 'my-host-2', id: -4 },
          { status: 'up', type: 'osd', name: 'osd.0-2', id: 4 }
        ]);
      });

      it('should have two root nodes', () => {
        expect(component.nodes).toEqual([
          {
            cdId: -3,
            children: [
              {
                children: [
                  {
                    id: component.nodes[0].children[0].children[0].id,
                    cdId: 4,
                    status: 'up',
                    type: 'osd',
                    name: 'osd.0-2 (osd)'
                  }
                ],
                id: component.nodes[0].children[0].id,
                cdId: -4,
                status: undefined,
                type: 'host',
                name: 'my-host-2 (host)'
              }
            ],
            id: component.nodes[0].id,
            status: undefined,
            type: 'root',
            name: 'default-2 (root)'
          },
          {
            children: [
              {
                children: [
                  {
                    id: component.nodes[1].children[0].children[0].id,
                    cdId: 0,
                    status: 'up',
                    type: 'osd',
                    name: 'osd.0 (osd)'
                  },
                  {
                    id: component.nodes[1].children[0].children[1].id,
                    cdId: 1,
                    status: 'down',
                    type: 'osd',
                    name: 'osd.1 (osd)'
                  },
                  {
                    id: component.nodes[1].children[0].children[2].id,
                    cdId: 2,
                    status: 'up',
                    type: 'osd',
                    name: 'osd.2 (osd)'
                  }
                ],
                id: component.nodes[1].children[0].id,
                cdId: -2,
                status: undefined,
                type: 'host',
                name: 'my-host (host)'
              }
            ],
            id: component.nodes[1].id,
            cdId: -1,
            status: undefined,
            type: 'root',
            name: 'default (root)'
          }
        ]);
      });
    });
  });
});
