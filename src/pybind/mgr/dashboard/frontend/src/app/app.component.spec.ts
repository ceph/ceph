import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';

import { AppComponent } from './app.component';
import { BlockModule } from './ceph/block/block.module';
import { ClusterModule } from './ceph/cluster/cluster.module';
import { CoreModule } from './core/core.module';
import { SharedModule } from './shared/shared.module';
import { configureTestBed } from './shared/unit-test-helper';

describe('AppComponent', () => {
  configureTestBed({
    imports: [
      RouterTestingModule,
      CoreModule,
      SharedModule,
      ToastModule.forRoot(),
      ClusterModule,
      BlockModule
    ],
    declarations: [AppComponent]
  });
});
