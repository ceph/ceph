import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { of } from 'rxjs';

import { TreeModule } from 'ng2-tree';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DashboardService } from '../../../shared/api/dashboard.service';
import { SharedModule } from '../../../shared/shared.module';
import { CrushmapComponent } from './crushmap.component';

describe('CrushmapComponent', () => {
  let component: CrushmapComponent;
  let fixture: ComponentFixture<CrushmapComponent>;
  let debugElement: DebugElement;
  configureTestBed({
    imports: [HttpClientTestingModule, TreeModule, TabsModule.forRoot(), SharedModule],
    declarations: [CrushmapComponent],
    providers: [DashboardService]
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
    const span = debugElement.nativeElement.querySelector('span');
    expect(span.textContent).toContain(component.panelTitle);
  });

  describe('test tree', () => {
    let dashboardService: DashboardService;
    const prepareGetHealth = (nodes: object[]) => {
      spyOn(dashboardService, 'getHealth').and.returnValue(
        of({ osd_map: { tree: { nodes: nodes } } })
      );
      fixture.detectChanges();
    };

    beforeEach(() => {
      dashboardService = debugElement.injector.get(DashboardService);
    });

    it('should display "No nodes!" if ceph tree nodes is empty array', () => {
      prepareGetHealth([]);
      expect(dashboardService.getHealth).toHaveBeenCalled();
      expect(component.tree.value).toEqual('No nodes!');
    });

    describe('nodes not empty', () => {
      beforeEach(() => {
        prepareGetHealth([
          { children: [-2], type: 'root', name: 'default', id: -1 },
          { children: [1, 0, 2], type: 'host', name: 'my-host', id: -2 },
          { status: 'up', type: 'osd', name: 'osd.0', id: 0 },
          { status: 'down', type: 'osd', name: 'osd.1', id: 1 },
          { status: 'up', type: 'osd', name: 'osd.2', id: 2 }
        ]);
      });

      it('should have tree structure derived from a root', () => {
        expect(component.tree.value).toBe('default (root)');
      });

      it('should have one host child with 3 osd children', () => {
        expect(component.tree.children.length).toBe(1);
        expect(component.tree.children[0].value).toBe('my-host (host)');
        expect(component.tree.children[0].children.length).toBe(3);
      });

      it('should have 3 osds in orderd', () => {
        expect(component.tree.children[0].children[0].value).toBe('osd.0 (osd)--up');
        expect(component.tree.children[0].children[1].value).toBe('osd.1 (osd)--down');
        expect(component.tree.children[0].children[2].value).toBe('osd.2 (osd)--up');
      });
    });
  });
});
