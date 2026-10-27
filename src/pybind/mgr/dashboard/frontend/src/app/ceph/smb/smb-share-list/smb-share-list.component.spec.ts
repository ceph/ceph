import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { of } from 'rxjs';
import { RouterTestingModule } from '@angular/router/testing';

import { SmbShareListComponent } from './smb-share-list.component';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideHttpClient } from '@angular/common/http';
import {
  SMB_CEPHFS_CONTEXT,
  SMB_CONTEXT,
  SMB_RGW_CONTEXT,
  SmbContext
} from '~/app/shared/constants/smb.constant';

import { SharedModule } from '~/app/shared/shared.module';
import { SmbService } from '~/app/shared/api/smb.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

const MIXED_SHARES = [
  {
    resource_type: 'ceph.smb.share',
    cluster_id: 'c1',
    share_id: 'cephfs-share',
    cephfs: { volume: 'vol1', path: '/' }
  },
  {
    resource_type: 'ceph.smb.share',
    cluster_id: 'c1',
    share_id: 'rgw-share',
    rgw: { bucket: 'photos' }
  }
] as any[];

describe('SmbShareListComponent', () => {
  let component: SmbShareListComponent;
  let fixture: ComponentFixture<SmbShareListComponent>;
  let smbService: SmbService;
  let rgwBucketService: RgwBucketService;

  const setup = async (ctx: SmbContext = SMB_CEPHFS_CONTEXT) => {
    await TestBed.configureTestingModule({
      imports: [SharedModule, RouterTestingModule],
      declarations: [SmbShareListComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: SMB_CONTEXT, useValue: ctx }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbShareListComponent);
    component = fixture.componentInstance;
    smbService = TestBed.inject(SmbService);
    rgwBucketService = TestBed.inject(RgwBucketService);
    fixture.detectChanges();
  };

  beforeEach(() => setup());

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should include the create share action', () => {
    expect(component.tableActions.map((action) => action.name)).toContain('Create');
  });

  it('should only show CephFS shares on the CephFS route', fakeAsync(() => {
    jest.spyOn(smbService, 'listShares').mockReturnValue(of(MIXED_SHARES));

    let rows: any[];
    component.smbShares$.subscribe((value) => (rows = value));
    component.loadSMBShares();
    tick();

    expect(rows.length).toBe(1);
    expect(rows[0].share_id).toBe('cephfs-share');
    expect(rows[0].cephfs).toBeDefined();
    expect(rows[0].rgw).toBeUndefined();
  }));

  it('should link RGW bucket names to the bucket resource page', async () => {
    TestBed.resetTestingModule();
    await setup(SMB_RGW_CONTEXT);
    const bucketColumn = component.columns.find((column) => column.prop === 'rgw.bucket');
    expect(bucketColumn.cellTransformation).toBe(CellTemplate.redirect);
    expect(bucketColumn.customTemplateConfig.customRowProperty).toBe('bucketRouterLink');

    jest.spyOn(smbService, 'listShares').mockReturnValue(of(MIXED_SHARES));
    jest
      .spyOn(rgwBucketService, 'list')
      .mockReturnValue(of([{ bid: 'photos', owner: 'alice' }]) as any);

    let rows: any[];
    component.smbShares$.subscribe((value) => (rows = value));
    component.loadSMBShares();

    expect(rows.length).toBe(1);
    expect(rows[0].share_id).toBe('rgw-share');
    expect(rows[0].bucketRouterLink).toEqual(['/rgw/bucket', 'photos', 'alice']);
  });
});
