import { CephfsMirroringPageHelper } from './mirroring.po';

describe('CephFS mirroring page', () => {
  const mirroring = new CephfsMirroringPageHelper();
  const fsName = 'e2e_mirror';
  const username = 'mirror';
  const siteName = 'site-b';
  const mirrorDir = 'e2e_data';
  const mirrorPath = `/${mirrorDir}`;

  const extraDir = 'e2e_docs';
  const extraPath = `/${extraDir}`;
  const subvolGroup = 'e2e_svgroup';
  const subvolName = 'e2e_sv';
  const subvolPath = `/volumes/${subvolGroup}/${subvolName}`;

  beforeEach(() => {
    cy.login();
  });

  describe('bootstrap two clusters', () => {
    it('should generate a token on the secondary and import it on the primary', () => {
      mirroring.isFilesystemMirrored(fsName).then((alreadyMirrored) => {
        if (alreadyMirrored) {
          cy.log(`${fsName} is already mirrored; skipping token import`);
          mirroring.visitPrimary();
          mirroring.expectMirroredFilesystem(fsName);
          return;
        }

        const ceph2Url: string = Cypress.env('CEPH2_URL');
        const ceph2Origin = new URL(ceph2Url).origin;
        const args = { fsName, username, siteName };

        mirroring.createFilesystem(fsName);
        cy.ceph2Login();
        mirroring.createFilesystem(fsName, ceph2Origin);

        cy.origin(ceph2Origin, { args }, ({ fsName, username, siteName }) => {
          cy.visit('/');
          cy.visit('#/cephfs/mirroring');
          cy.get('cd-login, cd-cephfs-mirroring-list').should('exist');
          cy.get('body').then(($body) => {
            if ($body.find('cd-login').length) {
              cy.get('#username').type('admin', { delay: 0 });
              cy.get('#password').type('admin', { delay: 0 });
              cy.get('[type=submit]').click();
              cy.get('cd-login').should('not.exist');
            }
          });
          cy.get('cd-cephfs-mirroring-list').should('exist');
          cy.contains('cd-clickable-tile', 'Prepare to receive').click();
          cy.get('cd-cephfs-generate-token').should('be.visible');
          cy.get(
            `cd-cephfs-generate-token cds-select[id=filesystem] option[value="${fsName}"]`
          ).should('exist');
          cy.get('cd-cephfs-generate-token cds-select[id=filesystem] select').select(fsName, {
            force: true
          });
          cy.get('cd-cephfs-generate-token #username').type(username, { delay: 0 });
          cy.get('cd-cephfs-generate-token #sitename').type(siteName, { delay: 0 });
          cy.get('cd-cephfs-generate-token [data-testid=submitBtn]').click();
          cy.get('cd-cephfs-download-token textarea#secureToken')
            .should('not.have.value', '')
            .invoke('val');
        }).then((bootstrapToken) => {
          expect(bootstrapToken, 'bootstrap token from secondary')
            .to.be.a('string')
            .and.not.equal('');
          // origin clears cookies / session storage on the primary cluster
          cy.login();
          mirroring.visitPrimary();
          mirroring.importToken(fsName, String(bootstrapToken));
          mirroring.expectMirroredFilesystem(fsName);
        });
      });
    });
  });

  describe('add mirror paths', () => {
    it('should add a mirrored path with a snapshot schedule', () => {
      mirroring.isFilesystemMirrored(fsName).then((fsMirrored) => {
        expect(fsMirrored, `${fsName} is mirrored`).to.eq(true);
        mirroring.ensureMirrorPathAbsent(fsName, mirrorPath);
        mirroring.createDirectory(fsName, mirrorPath);
        mirroring.openAddMirrorPath(fsName);
        mirroring.addPathWithHourlySchedule(mirrorDir, mirrorPath);
        mirroring.expectMirroredPathWithSchedule(fsName, mirrorPath);
      });
    });

    it('should add a directory and a subvolume path together after selecting the filesystem', () => {
      mirroring.isFilesystemMirrored(fsName).then((fsMirrored) => {
        expect(fsMirrored, `${fsName} is mirrored`).to.eq(true);
        mirroring.ensureMirrorPathAbsent(fsName, extraPath);
        mirroring.ensureMirrorPathAbsent(fsName, subvolPath);
        mirroring.createDirectory(fsName, extraPath);
        mirroring.createSubvolumeGroup(fsName, subvolGroup);
        mirroring.createSubvolume(fsName, subvolName, subvolGroup);
        mirroring.openAddMirrorPathFromList(fsName);
        mirroring.addPathsWithHourlySchedule(
          [[extraDir], ['volumes', subvolGroup, subvolName]],
          fsName
        );
        mirroring.expectMirroredPaths(fsName, [extraPath, subvolPath]);
      });
    });
  });

  describe('mirror path side panel', () => {
    it('should switch between the Details, Snapshots, and Schedule policy tabs', () => {
      mirroring.expectSidePanelTabs(fsName, mirrorPath);
    });
  });

  describe('remove mirror path', () => {
    it('should remove the mirrored path', () => {
      mirroring.removeMirrorPath(fsName, mirrorPath);
    });
  });

  describe('disable mirroring', () => {
    it('should disable mirroring on the primary filesystem', () => {
      mirroring.disableMirroring(fsName);
    });
  });
});
