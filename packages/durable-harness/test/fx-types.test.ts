import { expectTypeOf, it } from 'vitest';
import { fxTools } from '../src/fx.ts';

it("fxTools()'s tools say whether a call is safe to run twice, as createFxAgent() requires of its tools", () => {
  expectTypeOf<ReturnType<typeof fxTools>[number]['idempotent']>().toEqualTypeOf<boolean>();
});
