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

  let bootstrapSucceeded = false;
  let pathAddedSucceeded = false;
  let multiPathsSucceeded = false;

  beforeEach(() => {
    cy.login();
  });

  describe('bootstrap two clusters', () => {
    it('should generate a token on the secondary and import it on the primary', () => {
      mirroring.createFilesystem(fsName);

      const url: string = Cypress.env('CEPH2_URL');
      const args = { fsName, username, siteName };

      // Page objects cannot be used inside cy.origin. Always log in here:
      // the primary cy.session does not authenticate this iframe. Visit the
      // create page so AuthGuard's returnUrl returns to the form after login.
      // @ts-ignore
      cy.origin(url, { args }, ({ fsName, username, siteName }) => {
        cy.visit('#/cephfs/fs/create');
        cy.get('[name=username]').should('be.visible').type('admin', { delay: 0 });
        cy.get('#password').type('admin', { delay: 0 });
        cy.get('[type=submit]').click();
        cy.get('[name=username]').should('not.exist');
        cy.get('#name').should('be.visible').clear().type(fsName, { delay: 0 });
        cy.get('cd-submit-button').click();
        cy.get('cd-cephfs-list').should('exist');

        cy.visit('#/cephfs/mirroring');
        cy.get('cd-cephfs-mirroring-list').should('exist');
        cy.contains('cd-clickable-tile', 'Prepare to receive').click();
        cy.get('cd-cephfs-generate-token').should('be.visible');
        cy.get(`cd-cephfs-generate-token cds-select[id=filesystem] option[value="${fsName}"]`).should(
          'exist'
        );
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
        // origin clears cookies / session storage on the primary cluster
        cy.login();
        mirroring.navigateTo();
        mirroring.importToken(fsName, String(bootstrapToken));
        mirroring.expectMirroredFilesystem(fsName);
        cy.then(() => {
          bootstrapSucceeded = true;
        });
      });
    });
  });

  describe('add mirror path', () => {
    it('should add a mirrored path with a snapshot schedule', function () {
      if (!bootstrapSucceeded) {
        this.skip();
      }

      mirroring.enableSnapScheduleModule();
      cy.login();
      mirroring.createDirectory(fsName, mirrorPath);
      mirroring.openAddMirrorPath(fsName);
      mirroring.addPathWithHourlySchedule(mirrorDir, mirrorPath);
      mirroring.expectMirroredPathWithSchedule(fsName, mirrorPath);
      cy.then(() => {
        pathAddedSucceeded = true;
      });
    });
  });

  describe('add mirror paths from the list', () => {
    it('should add a directory and a subvolume path together after selecting the filesystem', function () {
      if (!bootstrapSucceeded) {
        this.skip();
      }

      mirroring.enableSnapScheduleModule();
      mirroring.createDirectory(fsName, extraPath);
      mirroring.createSubvolumeGroup(fsName, subvolGroup);
      mirroring.createSubvolume(fsName, subvolName, subvolGroup);
      cy.login();
      mirroring.openAddMirrorPathFromList(fsName);
      mirroring.addPathsWithHourlySchedule(
        [[extraDir], ['volumes', subvolGroup, subvolName]],
        fsName
      );
      mirroring.expectMirroredPaths(fsName, [extraPath, subvolPath]);
      cy.then(() => {
        multiPathsSucceeded = true;
      });
    });
  });

  describe('mirror path side panel', () => {
    it('should switch between the Details, Snapshots, and Schedule policy tabs', function () {
      const path = pathAddedSucceeded ? mirrorPath : extraPath;
      if (!pathAddedSucceeded && !multiPathsSucceeded) {
        this.skip();
      }

      mirroring.expectSidePanelTabs(fsName, path);
    });
  });

  describe('remove mirror path', () => {
    it('should remove the mirrored path', function () {
      if (!pathAddedSucceeded) {
        this.skip();
      }

      mirroring.removeMirrorPath(fsName, mirrorPath);
    });
  });

  describe('disable mirroring', () => {
    it('should disable mirroring on the primary filesystem', function () {
      if (!bootstrapSucceeded) {
        this.skip();
      }

      mirroring.disableMirroring(fsName);
    });
  });
});
