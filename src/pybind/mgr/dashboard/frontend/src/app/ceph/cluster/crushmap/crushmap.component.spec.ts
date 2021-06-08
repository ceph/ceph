import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';

import { TreeModule } from '@circlon/angular-tree-component';
import { of } from 'rxjs';

import { CrushRuleService } from '~/app/shared/api/crush-rule.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CrushmapComponent } from './crushmap.component';

describe('CrushmapComponent', () => {
  let component: CrushmapComponent;
  let fixture: ComponentFixture<CrushmapComponent>;
  let debugElement: DebugElement;
  let crushRuleService: CrushRuleService;
  let crushRuleServiceInfoSpy: jasmine.Spy;
  configureTestBed({
    imports: [HttpClientTestingModule, TreeModule, SharedModule],
    declarations: [CrushmapComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CrushmapComponent);
    component = fixture.componentInstance;
    debugElement = fixture.debugElement;
    crushRuleService = TestBed.inject(CrushRuleService);
    crushRuleServiceInfoSpy = spyOn(crushRuleService, 'getInfo');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display right title', () => {
    const span = debugElement.nativeElement.querySelector('.card-header');
    expect(span.textContent).toBe('CRUSH map viewer');
  });

  it('should display "No nodes!" if ceph tree nodes is empty array', fakeAsync(() => {
    crushRuleServiceInfoSpy.and.returnValue(of({ nodes: [] }));
    fixture.detectChanges();
    tick(5000);
    expect(crushRuleService.getInfo).toHaveBeenCalled();
    expect(component.nodes[0].name).toEqual('No nodes!');
    component.ngOnDestroy();
  }));

  it('should have two root nodes', fakeAsync(() => {
    crushRuleServiceInfoSpy.and.returnValue(
      of({
        nodes: [
          { children: [-2], type: 'root', name: 'default', id: -1 },
          { children: [1, 0, 2], type: 'host', name: 'my-host', id: -2 },
          { status: 'up', type: 'osd', name: 'osd.0', id: 0 },
          { status: 'down', type: 'osd', name: 'osd.1', id: 1 },
          { status: 'up', type: 'osd', name: 'osd.2', id: 2 },
          { children: [-4], type: 'datacenter', name: 'site1', id: -3 },
          { children: [4], type: 'host', name: 'my-host-2', id: -4 },
          { status: 'up', type: 'osd', name: 'osd.0-2', id: 4 }
        ],
        roots: [-1, -3, -6]
      })
    );
    fixture.detectChanges();
    tick(10000);
    expect(crushRuleService.getInfo).toHaveBeenCalled();
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
        type: 'datacenter',
        name: 'site1 (datacenter)'
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
    component.ngOnDestroy();
  }));
});
