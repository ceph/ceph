import { PageHelper } from '../page-helper.po';

export class ImagesPageHelper extends PageHelper {
  pages = {
    index: { url: '#/block/rbd', id: 'cd-rbd-list' },
    create: { url: '#/block/rbd/create', id: 'cd-rbd-form' }
  };

  // Creates a block image and fills in the name, pool, and size fields.
  // Then checks if the image is present in the Images table.
  createImage(name: string, pool: string, size: string) {
    this.navigateTo('create');

    cy.get('#name').type(name); // Enter in image name

    // Select image pool
    cy.contains('Loading...').should('not.exist');
    this.selectOption('pool', pool);
    cy.get('#pool').should('have.class', 'ng-valid'); // check if selected

    // Enter in the size of the image
    cy.get('#size').type(size);

    // Click the create button and wait for image to be made
    cy.contains('button', 'Create RBD').click();
    this.getFirstTableCell(name).should('exist');
  }

  editImage(name: string, pool: string, newName: string, newSize: string) {
    this.navigateEdit(name);

    // Wait until data is loaded
    cy.get('#pool').should('contain.value', pool);

    cy.get('#name').clear().type(newName);
    cy.get('#size').clear().type(newSize); // click the size box and send new size

    cy.contains('button', 'Edit RBD').click();

    this.getExpandCollapseElement(newName).click();
    cy.get('.table.table-striped.table-bordered').contains('td', newSize);
  }

  // Selects RBD image and moves it to the trash,
  // checks that it is present in the trash table
  moveToTrash(name: string) {
    // wait for image to be created
    cy.get('.datatable-body').first().should('not.contain.text', '(Creating...)');

    this.getFirstTableCell(name).click();

    // click on the drop down and selects the move to trash option
    cy.get('.table-actions button.dropdown-toggle').first().click();
    cy.get('button.move-to-trash').click();

    cy.contains('button', 'Move Image').should('be.visible').click();

    // Clicks trash tab
    cy.contains('.nav-link', 'Trash').click();
    this.getFirstTableCell(name).should('exist');
  }

  // Checks trash tab table for image and then restores it to the RBD Images table
  // (could change name if new name is given)
  restoreImage(name: string, newName?: string) {
    // clicks on trash tab
    cy.contains('.nav-link', 'Trash').click();

    // wait for table to load
    this.getFirstTableCell(name).click();
    cy.contains('button', 'Restore').click();

    // wait for pop-up to be visible (checks for title of pop-up)
    cy.get('cd-modal #name').should('be.visible');

    // If a new name for the image is passed, it changes the name of the image
    if (newName !== undefined) {
      // click name box and send new name
      cy.get('cd-modal #name').clear().type(newName);
    }

    cy.contains('button', 'Restore Image').click();

    // clicks images tab
    cy.contains('.nav-link', 'Images').click();

    this.getFirstTableCell(newName).should('exist');
  }

  // Enters trash tab and purges trash, thus emptying the trash table.
  // Checks if Image is still in the table.
  purgeTrash(name: string, pool?: string) {
    // clicks trash tab
    cy.contains('.nav-link', 'Trash').click();
    cy.contains('button', 'Purge Trash').click();

    // Check for visibility of modal container
    cy.get('.modal-header').should('be.visible');

    // If purgeing a specific pool, selects that pool if given
    if (pool !== undefined) {
      this.selectOption('poolName', pool);
      cy.get('#poolName').should('have.class', 'ng-valid'); // check if pool is selected
    }
    cy.get('#purgeFormButton').click();
    // Wait for image to delete and check it is not present

    this.getFirstTableCell(name).should('not.exist');
  }
}
