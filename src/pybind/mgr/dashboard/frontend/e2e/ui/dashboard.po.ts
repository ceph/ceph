import { $, $$, by, ElementFinder } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class DashboardPageHelper extends PageHelper {
  pages = {
    index: '/#/dashboard'
  };

  async infoGroupTitle(index: number): Promise<string> {
    return $$('.info-group-title')
      .get(index)
      .getText();
  }

  async clickInfoCardLink(cardName: string): Promise<void> {
    await $(`cd-info-card[cardtitle="${cardName}"]`)
      .element(by.linkText(cardName))
      .click();
  }

  async infoCard(indexOrTitle: number | string): Promise<ElementFinder> {
    let infoCards = $$('cd-info-card');
    if (typeof indexOrTitle === 'number') {
      if ((await infoCards.count()) <= indexOrTitle) {
        return Promise.reject(
          `No element found for index ${indexOrTitle}. Elements array has ` +
            `only ${await infoCards.count()} elements.`
        );
      }
      return infoCards.get(indexOrTitle);
    } else if (typeof indexOrTitle === 'string') {
      infoCards = infoCards.filter(
        async (e) => (await e.$('.card-title').getText()) === indexOrTitle
      );
      if ((await infoCards.count()) === 0) {
        return Promise.reject(`No element found for title "${indexOrTitle}"`);
      }
      return infoCards.first();
    }
  }

  async infoCardBodyText(
    infoCard: ElementFinder | Promise<ElementFinder> | string
  ): Promise<string> {
    let _infoCard: ElementFinder;
    if (typeof infoCard === 'string') {
      _infoCard = await this.infoCard(infoCard);
    } else {
      _infoCard = typeof infoCard.then === 'function' ? await infoCard : infoCard;
    }
    return _infoCard.$('.card-text').getText();
  }
}
