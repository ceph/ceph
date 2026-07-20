import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'carbon-components-angular';

import { NvmeofTabsComponent } from './nvmeof-tabs.component';
import { SharedModule } from '~/app/shared/shared.module';

describe('NvmeofTabsComponent', () => {
  let component: NvmeofTabsComponent;
  let fixture: ComponentFixture<NvmeofTabsComponent>;
  let router: Router;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofTabsComponent],
      imports: [RouterTestingModule, SharedModule, TabsModule]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofTabsComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should default activeTab to gateways', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/gateways');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.gateways);
  });

  it('should set activeTab to subsystems when URL contains subsystems', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/subsystems');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.subsystems);
  });

  it('should set activeTab to namespaces when URL contains namespaces', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/namespaces');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.namespaces);
  });

  it('should fallback to gateways when URL does not match any tab', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/unknown');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.gateways);
  });

  it('should navigate to correct path on tab selection', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.subsystems);
    expect(component.selectedTab).toBe(component.Tabs.subsystems);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/subsystems']);
  });

  it('should navigate to gateways on selecting gateways tab', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.gateways);
    expect(component.selectedTab).toBe(component.Tabs.gateways);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/gateways']);
  });

  it('should navigate to namespaces on selecting namespaces tab', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.namespaces);
    expect(component.selectedTab).toBe(component.Tabs.namespaces);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/namespaces']);
  });

  it('should expose TABS enum via Tabs getter', () => {
    const tabs = component.Tabs;
    expect(tabs.gateways).toBe('gateways');
    expect(tabs.subsystems).toBe('subsystems');
    expect(tabs.namespaces).toBe('namespaces');
  });
});
