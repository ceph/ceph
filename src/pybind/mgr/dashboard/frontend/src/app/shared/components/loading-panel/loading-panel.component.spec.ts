import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap';

import { LoadingPanelComponent } from './loading-panel.component';

describe('LoadingPanelComponent', () => {
  let component: LoadingPanelComponent;
  let fixture: ComponentFixture<LoadingPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LoadingPanelComponent ],
      imports: [ AlertModule.forRoot() ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadingPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
