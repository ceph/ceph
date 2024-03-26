/* tslint:disable*/
import { ServicesPageHelper } from '../../cluster/services.po';
import { NFSPageHelper } from '../../orchestrator/workflow/nfs/nfs-export.po';
import { BucketsPageHelper } from '../../rgw/buckets.po';
/* tslint:enable*/

describe('nfsExport page', () => {
  const nfsExport = new NFSPageHelper();
  const services = new ServicesPageHelper();
  const buckets = new BucketsPageHelper();
  const bucketName = 'e2e.nfs.bucket';
  // @TODO: uncomment this when a CephFS volume can be created through Dashboard.
  // const fsPseudo = '/fsPseudo';
  const rgwPseudo = '/rgwPseudo';
  const editPseudo = '/editPseudo';
  const backends = ['CephFS', 'Object Gateway'];
  const squash = 'no_root_squash';
  const client: object = { addresses: '192.168.0.10' };

  beforeEach(() => {
    cy.login();
    nfsExport.navigateTo();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      nfsExport.expectBreadcrumbText('NFS');
    });
  });

  describe('Create, edit and delete', () => {
    it('should create an NFS cluster', () => {
      services.navigateTo('create');

      services.addService('nfs');

      services.checkExist('nfs.testnfs', true);
      services.clickServiceTab('nfs.testnfs', 'Daemons');
      services.checkServiceStatus('nfs');
    });

    it('should create a nfs-export with RGW backend', () => {
      buckets.navigateTo('create');
      buckets.create(bucketName, 'dashboard');

      nfsExport.navigateTo();
      nfsExport.existTableCell(rgwPseudo, false);
      nfsExport.navigateTo('create');
      nfsExport.create(backends[1], squash, client, rgwPseudo, bucketName);
      nfsExport.existTableCell(rgwPseudo);
    });

    // @TODO: uncomment this when a CephFS volume can be created through Dashboard.
    // it('should create a nfs-export with CephFS backend', () => {
    //   nfsExport.navigateTo();
    //   nfsExport.existTableCell(fsPseudo, false);
    //   nfsExport.navigateTo('create');
    //   nfsExport.create(backends[0], squash, client, fsPseudo);
    //   nfsExport.existTableCell(fsPseudo);
    // });

    it('should show Clients', () => {
      nfsExport.clickTab('cd-nfs-details', rgwPseudo, 'Clients (1)');
      cy.get('cd-nfs-details').within(() => {
        nfsExport.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should edit an export', () => {
      nfsExport.editExport(rgwPseudo, editPseudo);

      nfsExport.existTableCell(editPseudo);
    });

    it('should delete exports and bucket', () => {
      nfsExport.delete(editPseudo);

      buckets.navigateTo();
      buckets.delete(bucketName);
    });
  });
});
