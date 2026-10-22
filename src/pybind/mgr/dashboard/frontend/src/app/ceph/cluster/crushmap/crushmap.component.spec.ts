import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

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
    imports: [HttpClientTestingModule, SharedModule],
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
    expect(span.textContent.trim()).toBe('CRUSH map viewer');
  });

  it('should display "No nodes!" if ceph tree nodes is empty array', fakeAsync(() => {
    crushRuleServiceInfoSpy.and.returnValue(of({ nodes: [] }));
    fixture.detectChanges();
    tick(5000);
    expect(crushRuleService.getInfo).toHaveBeenCalled();
    expect(component.nodes[0].label).toEqual('No nodes!');
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
    expect(component.nodes).not.toBeNull();
    expect(component.nodes).toHaveLength(2);
    expect(component.nodes[0]).toHaveProperty('labelContext', {
      name: 'site1 (datacenter)',
      status: undefined,
      type: 'datacenter'
    });
    expect(component.nodes[1]).toHaveProperty('labelContext', {
      name: 'default (root)',
      status: undefined,
      type: 'root'
    });

    component.ngOnDestroy();
  }));

  describe('expanded state across background refresh', () => {
    const crushmapInfo = () => ({
      nodes: [
        { children: [-2], type: 'root', name: 'default', id: -1 },
        { children: [0, 1], type: 'host', name: 'my-host', id: -2 },
        { status: 'up', type: 'osd', name: 'osd.0', id: 0 },
        { status: 'up', type: 'osd', name: 'osd.1', id: 1 }
      ],
      roots: [-1]
    });
    const hostNode = () => component.nodes[0].children[0];

    beforeEach(() => {
      // Return fresh objects on every poll, like the real API does.
      crushRuleServiceInfoSpy.and.callFake(() => of(crushmapInfo()));
    });

    it('should expand all nodes by default', fakeAsync(() => {
      fixture.detectChanges();
      tick(5000);
      expect(component.nodes[0].expanded).toBe(true);
      expect(hostNode().expanded).toBe(true);
      component.ngOnDestroy();
    }));

    it('should keep a collapsed node collapsed after refresh', fakeAsync(() => {
      fixture.detectChanges();
      tick(5000);
      component.onNodeToggle({ node: { id: '-2' } });
      const pollsBefore = crushRuleServiceInfoSpy.calls.count();
      tick(5000);
      // make sure the tree really was rebuilt from a new poll
      expect(crushRuleServiceInfoSpy.calls.count()).toBeGreaterThan(pollsBefore);
      expect(hostNode().expanded).toBe(false);
      expect(component.nodes[0].expanded).toBe(true);
      component.ngOnDestroy();
    }));

    it('should keep a re-expanded node expanded after refresh', fakeAsync(() => {
      fixture.detectChanges();
      tick(5000);
      component.onNodeToggle({ node: { id: '-2' } });
      component.onNodeToggle({ node: { id: '-2' } });
      tick(5000);
      expect(hostNode().expanded).toBe(true);
      component.ngOnDestroy();
    }));

    it('should handle nodetoggle events emitted by tree nodes', fakeAsync(() => {
      fixture.detectChanges();
      tick(5000);
      fixture.detectChanges();
      const rootTreeNode = debugElement.query(By.css('cds-tree-node'));
      expect(rootTreeNode).not.toBeNull();
      rootTreeNode.triggerEventHandler('nodetoggle', { node: { id: '-1' } });
      tick(5000);
      expect(component.nodes[0].expanded).toBe(false);
      component.ngOnDestroy();
    }));
  });
});
