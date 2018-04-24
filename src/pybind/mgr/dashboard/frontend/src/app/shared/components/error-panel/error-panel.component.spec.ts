import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap';

import { ErrorPanelComponent } from './error-panel.component';

describe('ErrorPanelComponent', () => {
  let component: ErrorPanelComponent;
  let fixture: ComponentFixture<ErrorPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ErrorPanelComponent ],
      imports: [ AlertModule.forRoot() ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ErrorPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
