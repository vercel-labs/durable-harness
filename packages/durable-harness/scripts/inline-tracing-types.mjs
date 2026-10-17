import { mkdirSync, readFileSync, readdirSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, resolve } from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { rollup } from 'rollup';
import { dts } from 'rollup-plugin-dts';

const root = fileURLToPath(new URL('..', import.meta.url));
const dist = resolve(root, 'dist');
const pkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
const libraryManifest = createRequire(import.meta.url).resolve('@vercel/agent-runs/package.json');
const library = JSON.parse(readFileSync(libraryManifest, 'utf8'));
const libraryTypes = resolve(dirname(libraryManifest), library.exports['.'].types);
const input = Object.fromEntries(Object.values(pkg.publishConfig.exports).flatMap(entry =>
  typeof entry === 'object' && entry.types
    ? [[entry.types.replace(/^\.\/dist\//, '').replace(/\.d\.ts$/, ''), resolve(root, entry.types)]]
    : []));

// tsup treats declared peers as external even with dts.resolve. Only this peer's types must be inlined.
const bundle = await rollup({
  input,
  external: id => !/^@vercel\/agent-runs(?:\/|$)/.test(id) && !id.startsWith('.') && !isAbsolute(id),
  plugins: [{ name: 'agent-runs-types', resolveId: id => id === '@vercel/agent-runs' ? libraryTypes : null }, dts({ respectExternal: true })],
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
