import { mkdirSync, readFileSync, readdirSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, resolve } from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { rollup } from 'rollup';
import { dts } from 'rollup-plugin-dts';

const root = fileURLToPath(new URL('..', import.meta.url));
const dist = resolve(root, 'dist');
const pkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
// The optional peers whose types the public types use: inlined (only what is used), so an app without the peer still type-checks.
// `@vercel/agent-runs` for the tracing types; `@vercel/sandbox` for `NetworkPolicy` (`sandbox({ network })`).
const INLINED = ['@vercel/agent-runs', '@vercel/sandbox'];
const require = createRequire(import.meta.url);
const typesOf = Object.fromEntries(INLINED.map(name => {
  const manifest = require.resolve(`${name}/package.json`);
  const exported = JSON.parse(readFileSync(manifest, 'utf8')).exports['.'];
  return [name, resolve(dirname(manifest), typeof exported.types === 'string' ? exported.types : exported.import.types)];
}));
const inlined = id => INLINED.some(name => id === name || id.startsWith(`${name}/`));
const input = Object.fromEntries(Object.values(pkg.publishConfig.exports).flatMap(entry =>
  typeof entry === 'object' && entry.types
    ? [[entry.types.replace(/^\.\/dist\//, '').replace(/\.d\.ts$/, ''), resolve(root, entry.types)]]
    : []));

// tsup treats declared peers as external even with dts.resolve. Only these peers' types must be inlined.
const bundle = await rollup({
  input,
  external: id => !inlined(id) && !id.startsWith('.') && !isAbsolute(id),
  plugins: [{ name: 'optional-peer-types', resolveId: id => typesOf[id] ?? null }, dts({ respectExternal: true })],
  onwarn(warning, warn) { if (warning.code === 'UNRESOLVED_IMPORT') throw new Error(warning.message); warn(warning); },
});
try {
  const { output } = await bundle.generate({ format: 'es', entryFileNames: '[name].js', chunkFileNames: 'types-[hash].js' });
  const previous = readdirSync(dist, { recursive: true }).filter(file => file.endsWith('.d.ts'));
  for (const file of previous) unlinkSync(resolve(dist, file));
  for (const file of output) {
    const path = resolve(dist, file.fileName.replace(/\.js$/, '.d.ts'));
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, file.type === 'chunk' ? file.code : file.source);
  }
} finally { await bundle.close(); }
