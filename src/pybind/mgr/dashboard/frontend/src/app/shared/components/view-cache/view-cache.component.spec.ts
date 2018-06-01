import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../unit-test-helper';
import { ViewCacheComponent } from './view-cache.component';

describe('ViewCacheComponent', () => {
  let component: ViewCacheComponent;
  let fixture: ComponentFixture<ViewCacheComponent>;

  configureTestBed({
    declarations: [ViewCacheComponent],
    imports: [AlertModule.forRoot()]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ViewCacheComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
