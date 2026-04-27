import { RedirectLinkResolverPipe } from './redirect-link-resolver.pipe';

describe('RedirectLinkResolverPipe', () => {
  let pipe: RedirectLinkResolverPipe;

  beforeEach(() => {
    pipe = new RedirectLinkResolverPipe();
  });

  it('should create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('should replace "::prop" with the provided value', () => {
    const redirectLink = ['home', '::prop', 'details'];
    const value = 'route';
    const result = pipe.transform(redirectLink, value);
    expect(result).toEqual(['home', 'route', 'details']);
  });

  it('should handle multiple "::prop" replacements', () => {
    const redirectLink = ['::prop', 'user', '::prop'];
    const value = 'id42';
    const result = pipe.transform(redirectLink, value);
    expect(result).toEqual(['id42', 'user', 'id42']);
  });

  it('should return the same array if no "::prop" exists', () => {
    const redirectLink = ['home', 'about', 'contact'];
    const value = 'ignored';
    const result = pipe.transform(redirectLink, value);
    expect(result).toEqual(redirectLink);
  });
});
