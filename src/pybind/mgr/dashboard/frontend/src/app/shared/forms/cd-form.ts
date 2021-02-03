export enum LoadingStatus {
  Loading,
  Ready,
  Error,
  None
}

export class CdForm {
  loading = LoadingStatus.Loading;

  loadingStart() {
    this.loading = LoadingStatus.Loading;
  }

  loadingReady() {
    this.loading = LoadingStatus.Ready;
  }

  loadingError() {
    this.loading = LoadingStatus.Error;
  }

  loadingNone() {
    this.loading = LoadingStatus.None;
  }
}
