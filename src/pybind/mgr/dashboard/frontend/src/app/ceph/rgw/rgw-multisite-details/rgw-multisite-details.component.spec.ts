import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TreeModule } from '@circlon/angular-tree-component';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';

import { RgwMultisiteDetailsComponent } from './rgw-multisite-details.component';
import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwMultisiteDetailsComponent', () => {
  let component: RgwMultisiteDetailsComponent;
  let fixture: ComponentFixture<RgwMultisiteDetailsComponent>;
  let debugElement: DebugElement;

  configureTestBed({
    declarations: [RgwMultisiteDetailsComponent],
    imports: [
      HttpClientTestingModule,
      TreeModule,
      SharedModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteDetailsComponent);
    component = fixture.componentInstance;
    debugElement = fixture.debugElement;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display right title', () => {
    const span = debugElement.nativeElement.querySelector('.card-header');
    expect(span.textContent).toBe('Topology Viewer');
  });
});
