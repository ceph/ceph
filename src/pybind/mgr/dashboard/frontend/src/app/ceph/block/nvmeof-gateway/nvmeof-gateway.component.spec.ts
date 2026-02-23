import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';

import { NvmeofGatewayComponent } from './nvmeof-gateway.component';

import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ComboBoxModule, GridModule, TabsModule } from 'carbon-components-angular';
import { of } from 'rxjs';
import { BreadcrumbService } from '~/app/shared/services/breadcrumb.service';

describe('NvmeofGatewayComponent', () => {
  let component: NvmeofGatewayComponent;
  let fixture: ComponentFixture<NvmeofGatewayComponent>;
  let breadcrumbService: BreadcrumbService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofGatewayComponent],
      imports: [
        HttpClientModule,
        RouterTestingModule,
        SharedModule,
        ComboBoxModule,
        GridModule,
        TabsModule
      ],
      providers: [
        BreadcrumbService,
        {
          provide: ActivatedRoute,
          useValue: {
            queryParams: of({})
          }
        }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayComponent);
    component = fixture.componentInstance;
    breadcrumbService = TestBed.inject(BreadcrumbService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set tab crumb on init', () => {
    spyOn(breadcrumbService, 'setTabCrumb');
    component.ngOnInit();
    expect(breadcrumbService.setTabCrumb).toHaveBeenCalledWith('Gateways');
  });

  it('should update tab crumb on tab switch', () => {
    spyOn(breadcrumbService, 'setTabCrumb');
    component.onSelected(component.Tabs.subsystem);
    expect(breadcrumbService.setTabCrumb).toHaveBeenCalledWith('Subsystem');
  });

  it('should clear tab crumb on destroy', () => {
    spyOn(breadcrumbService, 'clearTabCrumb');
    component.ngOnDestroy();
    expect(breadcrumbService.clearTabCrumb).toHaveBeenCalled();
  });
});
