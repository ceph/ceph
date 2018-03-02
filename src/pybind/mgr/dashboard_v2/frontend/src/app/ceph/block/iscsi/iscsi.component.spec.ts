import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AppModule } from '../../../app.module';
import { TcmuIscsiService } from '../../../shared/services/tcmu-iscsi.service';
import { IscsiComponent } from './iscsi.component';

describe('IscsiComponent', () => {
  let component: IscsiComponent;
  let fixture: ComponentFixture<IscsiComponent>;

  const fakeService = {
    tcmuiscsi: () => {
      return new Promise(function(resolve, reject) {
        return;
      });
    },
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        imports: [AppModule],
        providers: [{ provide: TcmuIscsiService, useValue: fakeService }]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
