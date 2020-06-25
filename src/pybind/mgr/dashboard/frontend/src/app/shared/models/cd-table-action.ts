import { CdTableSelection } from './cd-table-selection';

export class CdTableAction {
  // It's possible to assign a string
  // or a function that returns the link if it has to be dynamic
  // or none if it's not needed
  routerLink?: string | Function;

  preserveFragment? = false;

  // This is the function that will be triggered on a click event if defined
  click?: Function;

  permission: 'create' | 'update' | 'delete' | 'read';

  // The name of the action
  name: string;

  // The font awesome icon that will be used
  icon: string;

  // You can define the condition to disable the action.
  // By default all 'update' and 'delete' actions will only be enabled
  // if one selection is made and no task is running on the selected item.
  disable?: (_: CdTableSelection) => boolean;

  /**
   * In some cases you might want to give the user a hint why a button is
   * disabled. The specified message will be shown to the user as a button
   * tooltip.
   */
  disableDesc?: (_: CdTableSelection) => string | undefined;

  /**
   * Defines if the button can become 'primary' (displayed as button and not
   * 'hidden' in the menu). Only one button can be primary at a time. By
   * default all 'create' actions can be the action button if no or multiple
   * items are selected. Also, all 'update' and 'delete' actions can be the
   * action button by default, provided only one item is selected.
   */
  canBePrimary?: (_: CdTableSelection) => boolean;

  // In some rare cases you want to hide a action that can be used by the user for example
  // if one action can lock the item and another action unlocks it
  visible?: (_: CdTableSelection) => boolean;
}
