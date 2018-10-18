import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs/tabs.module';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ConfigurationService } from '../../../shared/api/configuration.service';
import { SharedModule } from '../../../shared/shared.module';
import { ConfigurationDetailsComponent } from './configuration-details/configuration-details.component';
import { ConfigurationComponent } from './configuration.component';

describe('ConfigurationComponent', () => {
  let component: ConfigurationComponent;
  let fixture: ComponentFixture<ConfigurationComponent>;

  configureTestBed({
    declarations: [ConfigurationComponent, ConfigurationDetailsComponent],
    providers: [ConfigurationService],
    imports: [
      SharedModule,
      FormsModule,
      TabsModule.forRoot(),
      HttpClientTestingModule,
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
