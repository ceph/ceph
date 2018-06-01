import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AppModule } from '../../../app.module';
import { TcmuIscsiService } from '../../../shared/api/tcmu-iscsi.service';
import { configureTestBed } from '../../../shared/unit-test-helper';
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

  configureTestBed({
    imports: [AppModule],
    providers: [{ provide: TcmuIscsiService, useValue: fakeService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
