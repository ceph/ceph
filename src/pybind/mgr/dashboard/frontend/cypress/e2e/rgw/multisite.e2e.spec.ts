import { MultisitePageHelper } from './multisite.po';

describe('Multisite page', () => {
  const multisite = new MultisitePageHelper();

  beforeEach(() => {
    cy.login();
    multisite.navigateTo();
  });

  describe('tabs and table tests', () => {
    it('should show two tabs', () => {
      multisite.getTabsCount().should('eq', 2);
    });

    it('should show Configuration tab as a first tab', () => {
      multisite.getTabText(0).should('eq', 'Configuration');
    });

    it('should show sync policy tab as a second tab', () => {
      multisite.getTabText(1).should('eq', 'Sync Policy');
    });
  });

  describe('create, edit & delete sync group policy', () => {
    it('should create policy', () => {
      multisite.navigateTo('create');
      multisite.create('test', 'Enabled');
      multisite.getFirstTableCell('test').should('exist');
    });

    it('should edit policy status', () => {
      multisite.edit('test', 'Forbidden');
    });

    it('should delete policy', () => {
      multisite.getTab('Sync Policy').click();
      multisite.delete('test');
    });
  });

  describe('create, edit & delete symmetrical sync Flow', () => {
    it('Preparing...(creating sync group policy)', () => {
      multisite.navigateTo('create');
      multisite.create('test', 'Enabled');
      multisite.getFirstTableCell('test').should('exist');
    });
    describe('symmetrical Flow creation started', () => {
      beforeEach(() => {
        multisite.getTab('Sync Policy').click();
        multisite.getExpandCollapseElement().click();
      });

      it('should create flow', () => {
        multisite.createSymmetricalFlow('new-sym-flow', ['zone1-zg1-realm1']);
      });

      it('should modify flow zones', () => {
        multisite.editSymFlow('new-sym-flow', 'zone2-zg1-realm1');
      });

      it('should delete flow', () => {
        multisite.deleteSymFlow('new-sym-flow');
      });
    });
  });

  describe('create, edit & delete directional sync Flow', () => {
    beforeEach(() => {
      multisite.getTab('Sync Policy').click();
      multisite.getExpandCollapseElement().click();
    });

    it('should create flow', () => {
      multisite.createDirectionalFlow(
        'new-dir-flow',
        ['zone1-zg1-realm1', 'zone2-zg1-realm1'],
        ['new-zone']
      );
    });
  });

  describe('create, edit, delete pipe', () => {
    beforeEach(() => {
      multisite.getTab('Sync Policy').click();
      multisite.getExpandCollapseElement().click();
    });

    it('should create pipe', () => {
      multisite.createPipe('new-pipe', ['zone1-zg1-realm1'], ['zone3-zg2-realm1']);
    });

    it('should modify pipe zones', () => {
      multisite.editPipe('new-pipe', 'zone2-zg1-realm1');
    });

    it('should delete pipe', () => {
      multisite.deletePipe('new-pipe');
    });
  });
});
