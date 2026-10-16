import { describe, expect, it } from 'vitest';
import { NOT_RUN, NotRunError } from './sandbox.ts';

describe('NotRunError', () => {
  it('says "try again" only for what a retry can fix, and names any other cause (a missing project link)', () => {
    const yielded = Object.assign(new Error('contended'), { name: 'Yield' });
    expect(new NotRunError().message).toBe(NOT_RUN);
    expect(new NotRunError(yielded).message).toBe(NOT_RUN);
    expect(new NotRunError(new Error('the link was lost'), true).message).toBe(NOT_RUN);
    const missing = new NotRunError(new Error('harness-sandbox: no Vercel project for the sandbox: run `vercel link`'));
    expect(missing.message).toBe('Not run: the sandbox could not be attached (harness-sandbox: no Vercel project for the sandbox: run `vercel link`)');
    expect(missing.code).toBe('ENOTRUN');
  });
});
