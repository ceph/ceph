export interface DeleteConfirmationBodyContext {
  warningMessage?: string;
  inputLabel?: string;
  inputPlaceholder?: string;
  deletionMessage?: string;
  confirmHeading?: string;
  /** When set on a high-impact delete, user must check an extra acknowledgement before submit. */
  forceDeleteAcknowledgementMessage?: string;
}
