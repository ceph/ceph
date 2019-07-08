import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { PrometheusTabsComponent } from './prometheus-tabs.component';

describe('PrometheusTabsComponent', () => {
  let component: PrometheusTabsComponent;
  let fixture: ComponentFixture<PrometheusTabsComponent>;
  let router: Router;

  const selectTab = (index) => {
    fixture.debugElement.queryAll(By.css('tab'))[index].triggerEventHandler('select', null);
  };

  configureTestBed({
    declarations: [PrometheusTabsComponent],
    imports: [RouterTestingModule, HttpClientTestingModule, TabsModule.forRoot()]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PrometheusTabsComponent);
    component = fixture.componentInstance;
    router = TestBed.get(Router);
    spyOn(router, 'navigate').and.stub();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should redirect to alert listing', () => {
    selectTab(0);
    expect(router.navigate).toHaveBeenCalledWith(['/alerts']);
  });

  it('should redirect to silence listing', () => {
    selectTab(1);
    expect(router.navigate).toHaveBeenCalledWith(['/silence']);
  });
});
