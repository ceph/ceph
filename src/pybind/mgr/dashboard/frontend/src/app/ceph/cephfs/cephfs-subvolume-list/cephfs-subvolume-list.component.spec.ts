import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, FormGroup } from '@angular/forms';

import { CephfsSubvolumeListComponent } from './cephfs-subvolume-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CephfsSubvolumeListComponent', () => {
  let component: CephfsSubvolumeListComponent;
  let fixture: ComponentFixture<CephfsSubvolumeListComponent>;

  configureTestBed({
    declarations: [CephfsSubvolumeListComponent],
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('renders the retain snapshots option in the delete modal with carbon checkbox markup', () => {
    const form = new FormGroup({
      child: new FormGroup({
        retainSnapshots: new FormControl(false)
      })
    });
    const view = component.removeTmpl.createEmbeddedView({ form });
    const host = document.createElement('div');

    view.detectChanges();
    view.rootNodes.forEach((node) => {
      if (node instanceof Node) {
        host.appendChild(node);
      }
    });

    expect(host.querySelector('cds-checkbox#retainSnapshots')).not.toBeNull();
    expect(host.textContent).toContain('Retain snapshots');
    expect(host.querySelector('cd-helper')?.textContent).toContain(
      'The subvolume can be removed retaining existing snapshots using this option.'
    );
    expect(host.querySelector('.custom-control')).toBeNull();

    view.destroy();
  });
});
