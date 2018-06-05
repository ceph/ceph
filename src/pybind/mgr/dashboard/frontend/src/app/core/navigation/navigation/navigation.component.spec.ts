import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AppModule } from '../../../app.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { NavigationComponent } from './navigation.component';

describe('NavigationComponent', () => {
  let component: NavigationComponent;
  let fixture: ComponentFixture<NavigationComponent>;

  configureTestBed({
    imports: [AppModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
