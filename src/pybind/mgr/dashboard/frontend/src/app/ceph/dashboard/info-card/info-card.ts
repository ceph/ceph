import {InfoCardAdditionalInfo} from "./info-card-additional-info";

export class InfoCard {
  public titleLink?: string;
  public titleImageClass?: string;
  public description?: string;
  public message?: string;
  public messageClass?: string = '';
  public messageStyle?: any;
  public additionalInfo?: InfoCardAdditionalInfo[];

  constructor(public title: string) {}
}
