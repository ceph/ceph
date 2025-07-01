export class CssHelper {
  /**
   * Gets the value of a CSS custom property (CSS variable).
   * @param propertyName The name of the variable without `--`.
   * @param element Optional: HTMLElement to scope the variable lookup. Defaults to `document.body`.
   */
  propertyValue(propertyName: string, element?: HTMLElement): string {
    const target = element ?? document.body;
    return getComputedStyle(target).getPropertyValue(`--${propertyName}`);
  }
}
