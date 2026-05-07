export interface DeleteConfirmationBodyContext {
  warningMessage?: string;
  disableForm?: boolean;
  inputLabel?: string;
  inputPlaceholder?: string;
  deletionMessage?: string;
  /** When set on a high-impact delete, user must check an extra acknowledgement before submit. */
  forceDeleteAcknowledgementMessage?: string;
}
