export class CssHelper {
  propertyValue(propertyName: string): string {
    return getComputedStyle(document.body).getPropertyValue(`--${propertyName}`);
  }
}
