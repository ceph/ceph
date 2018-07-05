export class InfoCard {
  public title: string;
  public titleLink?: string;
  public titleImageClass?: string;
  public titleI18n?: string;
  public info?: string;
  public infoClass?: string;
  public additionalInfo?: string[];

  constructor(title: string) {
    this.title = title;
  }
}
