import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { AlertModule, BsDropdownModule, TabsModule } from 'ngx-bootstrap';

import { ComponentsModule } from '../../../shared/components/components.module';
import { SharedModule } from '../../../shared/shared.module';
import { PoolDetailComponent } from './pool-detail.component';

describe('PoolDetailComponent', () => {
  let component: PoolDetailComponent;
  let fixture: ComponentFixture<PoolDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        BsDropdownModule.forRoot(),
        TabsModule.forRoot(),
        AlertModule.forRoot(),
        ComponentsModule,
        RouterTestingModule,
        HttpClientTestingModule
      ],
      declarations: [ PoolDetailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
