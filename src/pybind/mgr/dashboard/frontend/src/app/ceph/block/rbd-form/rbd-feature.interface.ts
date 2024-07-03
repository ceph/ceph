export interface RbdImageFeature {
  desc: string;
  allowEnable: boolean;
  allowDisable: boolean;
  requires?: string;
  interlockedWith?: string;
  key?: string;
  initDisabled?: boolean;
  helperHtml?: string;
  helperText?: string;
}
