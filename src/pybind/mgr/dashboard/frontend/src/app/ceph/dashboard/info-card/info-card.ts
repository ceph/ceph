import {InfoCardAdditionalInfo} from "./info-card-additional-info";

export class InfoCard {
  public titleLink?: string;
  public titleImageClass?: string;
  public info?: string;
  public infoClass?: string;
  public infoStyle?: any;
  public additionalInfo?: InfoCardAdditionalInfo[];

  constructor(public title: string) {}
}
