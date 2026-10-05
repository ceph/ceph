import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NfsClusterComponent } from './nfs-cluster.component';

describe('NfsClusterComponent', () => {
  let component: NfsClusterComponent;
  let fixture: ComponentFixture<NfsClusterComponent>;

  configureTestBed({
    declarations: [NfsClusterComponent],
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsClusterComponent);
    component = fixture.componentInstance;
  });
  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should link cluster name to the cephfs cluster overview page', () => {
    Object.defineProperty(TestBed.inject(Router), 'url', {
      get: () => '/cephfs/nfs/cluster'
    });
    component.ngOnInit();
    expect(component.columns[0].prop).toBe('name');
    expect(component.columns[0].cellTransformation).toBe(CellTemplate.redirect);
    expect(component.columns[0].customTemplateConfig).toEqual({
      redirectLink: ['/cephfs/nfs/cluster', '::prop', 'overview']
    });
  });

  it('should link cluster name to the rgw cluster overview page', () => {
    Object.defineProperty(TestBed.inject(Router), 'url', {
      get: () => '/rgw/nfs/cluster'
    });
    component.ngOnInit();
    expect(component.columns[0].customTemplateConfig).toEqual({
      redirectLink: ['/rgw/nfs/cluster', '::prop', 'overview']
    });
  });
});
