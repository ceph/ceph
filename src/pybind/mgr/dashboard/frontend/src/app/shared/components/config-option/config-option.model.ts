export class ConfigFormModel {
  name: string;
  desc: string;
  long_desc: string;
  type: string;
  value: Array<any>;
  default: any;
  daemon_default: any;
  min: any;
  max: any;
  services: Array<string>;
}
