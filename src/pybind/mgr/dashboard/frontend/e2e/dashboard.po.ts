import { $, by, element } from 'protractor';
import { PageHelper } from './page-helper.po';

export class DashboardPageHelper extends PageHelper {
  pages = {
    index: '/#/dashboard'
  };

  checkGroupTitles(index, name) {
    // Checks that the titles of all the groups on the dashboard are correct
    const titles = element.all(by.className('info-group-title'));
    titles
      .get(index)
      .getText()
      .then((txt) => {
        expect(txt === name);
      });
  }

  cellFromGroup(cardName) {
    // Grabs cell from dashboard page based off the title. Then returns the card
    // element
    return $(`cd-info-card[cardtitle=${cardName}]`);
  }

  cellLink(cardName) {
    // Grabs the link from the correct card using the cellFromGroup function,
    // then clicks the hyperlinked title
    this.navigateTo();
    this.cellFromGroup(cardName)
      .element(by.linkText(cardName))
      .click();
  }

  partialCellLink(partName) {
    // Used for cases in which there was a space inbetween two words in the hyperlink,
    // has the same functionality as cellLink
    element(by.partialLinkText(partName)).click();
  }

  dashContain(index, name) {
    // Grabs a list of all info cards, then checks by index that the title text
    // is equal to the desired title, thus checking the presence of the card
    const cardList = element.all(by.tagName('cd-info-card'));
    cardList
      .get(index)
      .getText()
      .then((txt) => {
        expect(txt === name);
      });
  }

  cardNumb(index) {
    // Grabs a list of all info cards and returns the text on the card via
    // the index of the card in the list
    const cardList = element.all(by.tagName('cd-info-card'));
    return cardList.get(index).getText();
  }
}
