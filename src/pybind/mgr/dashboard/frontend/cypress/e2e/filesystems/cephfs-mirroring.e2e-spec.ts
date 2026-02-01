import { CephfsMirroringPageHelper } from './cephfs-mirroring.po';

describe('CephFS Mirroring', () => {
  const cephfsMirroring = new CephfsMirroringPageHelper();

  beforeEach(() => {
    cy.login();
  });

  describe('mirroring list page', () => {
    beforeEach(() => {
      cephfsMirroring.navigateToList();
    });

    it('should open and show breadcrumb', () => {
      cephfsMirroring.expectBreadcrumbText('Mirroring');
    });

    it('should show the mirroring list component', () => {
      cy.get('cd-cephfs-mirroring-list').should('exist');
    });

    it('should show the mirroring daemon status table', () => {
      cy.get('cd-cephfs-mirroring-list cd-table').should('exist');
    });
  });

  describe('mirroring paths page', () => {
    const fsName = 'fs1';

    beforeEach(() => {
      cephfsMirroring.navigateToPaths(fsName);
    });

    it('should open and show breadcrumb File / Mirroring / fsName / paths', () => {
      cephfsMirroring.expectBreadcrumbText('paths');
      cephfsMirroring.expectBreadcrumbSegments(['File', 'Mirroring', fsName, 'paths']);
    });

    it('should show the mirroring paths component', () => {
      cy.get('cd-cephfs-mirroring-paths').should('exist');
    });

    it('should show Local filesystem, Remote filesystem and Remote cluster sections', () => {
      cy.get('cd-cephfs-mirroring-paths').within(() => {
        cy.contains('Local filesystem').should('be.visible');
        cy.contains('Remote filesystem').should('be.visible');
        cy.contains('Remote cluster').should('be.visible');
      });
    });

    it('should show Mirroring paths table section', () => {
      cy.get('cd-cephfs-mirroring-paths').within(() => {
        cy.contains('Mirroring paths').should('be.visible');
        cy.get('cd-table').should('exist');
      });
    });
  });

  describe('navigation from list to paths', () => {
    const mockFsName = 'fs1';
    const mockDaemonStatus = [
      {
        daemon_id: 1,
        filesystems: [
          {
            filesystem_id: 10,
            name: mockFsName,
            directory_count: 1,
            peers: [
              {
                uuid: 'peer-uuid',
                remote: {
                  client_name: 'client.mirror',
                  cluster_name: 'remote-cluster',
                  fs_name: 'remote-fs'
                },
                stats: { failure_count: 0, recovery_count: 0 }
              }
            ],
            id: '1-10'
          }
        ]
      }
    ];

    beforeEach(() => {
      cy.intercept('GET', '**/api/cephfs/mirror/daemon-status', {
        statusCode: 200,
        body: mockDaemonStatus
      }).as('daemonStatus');
      cephfsMirroring.navigateToList();
      cy.wait('@daemonStatus');
    });

    it('should navigate to paths when clicking local filesystem link in a row', () => {
      cy.get('cd-cephfs-mirroring-list').within(() => {
        cy.get('[cdstablerow]').should('have.length.at.least', 1);
      });
      cephfsMirroring.clickLocalFsLink(mockFsName);
      cy.url().should('include', `/cephfs/mirroring/${mockFsName}`);
      cy.get('cd-cephfs-mirroring-paths').should('exist');
      cephfsMirroring.expectBreadcrumbSegments(['File', 'Mirroring', mockFsName]);
      cy.get('cd-cephfs-mirroring-paths').within(() => {
        cy.contains('Local filesystem').should('be.visible');
        cy.contains(mockFsName).should('be.visible');
        cy.contains('Mirroring paths').should('be.visible');
      });
    });
  });
});
