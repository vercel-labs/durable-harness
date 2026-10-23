import { parse } from '@babel/parser';
import traverseModule, { type Binding, type NodePath } from '@babel/traverse';

// Babel's CommonJS entry has a default property when loaded by Node's ESM
// loader; bundlers may unwrap it already.
const traverse = 'default' in traverseModule ? traverseModule.default as typeof traverseModule : traverseModule;

type Reference = { source: string; name?: string };
export interface HarnessModule {
  agent: boolean;
  /** A real factory call, even without an export: classifies explicitly listed modules. */
  agentFactory: boolean;
  piOpener: boolean;
  piSessions: boolean;
}

const FACTORIES: Readonly<Record<string, string>> = {
  'durable-harness': 'durableAgent',
  'durable-harness/claude-code': 'claudeCode',
  'durable-harness/codex': 'codex',
  'durable-harness/opencode': 'opencode',
  'durable-harness/pi': 'pi',
};
const PI = 'durable-harness/pi-durable';

/**
 * Read actual imports, calls and exports without loading the app. Automatic
 * discovery follows exported values back to agents made at module scope;
 * scripts and exported factories are not registration modules. Explicit
 * `agents` handles factories or registrations this static check cannot see.
 * Pi's resumeWith is deliberately different: it registers an opener for its
 * side effect, even when its return value is not exported.
 */
export function harnessModule(text: string): HarnessModule {
  const found: HarnessModule = { agent: false, agentFactory: false, piOpener: false, piSessions: false };
  if (!text.includes('durable-harness')) return found;
  let ast: ReturnType<typeof parse>;
  const options = { sourceType: 'unambiguous', allowUndeclaredExports: true, createImportExpressions: true } as const;
  try {
    ast = parse(text, { ...options, plugins: ['typescript', 'jsx', 'decorators-legacy'] });
  } catch {
    // A .ts angle-bracket assertion or generic arrow may be ambiguous with
    // JSX. No source extension is needed to handle both kinds of module.
    try {
      ast = parse(text, { ...options, plugins: ['typescript', 'decorators-legacy'] });
    } catch {
      return found;
    }
  }
  traverse(ast, {
    'CallExpression|OptionalCallExpression'(path) {
      const callee = path.get('callee') as NodePath;
      const imported = reference(callee);
      if (!imported) return;
      if (imported.source === PI) {
        if (['openWorldStorage', 'acquireHarness', 'chatRoute'].includes(imported.name ?? '')) found.piSessions = true;
        if (imported.name === 'resumeWith' && atModuleScope(path)) found.piOpener = found.agent = true;
      } else if (FACTORIES[imported.source] === imported.name && imported.name) {
        found.agentFactory = true;
        if (atModuleScope(path) && reachesExport(path)) found.agent = true;
      }
    },
  });
  return found;
}

/** Constructors and instance fields, like functions, do not run at import. */
function atModuleScope(path: NodePath): boolean {
  return !path.findParent((parent) => parent.isFunction() || parent.isClass());
}

/** Follow a value import, namespace member, or local alias to its module. */
function reference(path: NodePath, seen = new Set<Binding>()): Reference | undefined {
  if (path.isAwaitExpression()) return reference(path.get('argument'), seen);
  if (path.isTSAsExpression() || path.isTSSatisfiesExpression() || path.isTSNonNullExpression() || path.isTSTypeAssertion() || path.isParenthesizedExpression()) return reference(path.get('expression') as NodePath, seen);
  if (path.isImportExpression() && path.node.source.type === 'StringLiteral') return { source: path.node.source.value };
  if (path.isCallExpression() && path.node.callee.type === 'Identifier' && path.node.callee.name === 'require' && !path.scope.getBinding('require') && path.node.arguments[0]?.type === 'StringLiteral') return { source: path.node.arguments[0].value };
  if (path.isMemberExpression() || path.isOptionalMemberExpression()) {
    const object = reference(path.get('object') as NodePath, seen);
    const property = path.node.property;
    const name = !path.node.computed && property.type === 'Identifier' ? property.name : property.type === 'StringLiteral' ? property.value : undefined;
    return object && !object.name && name ? { source: object.source, name } : undefined;
  }
  if (!path.isIdentifier()) return undefined;
  const binding = path.scope.getBinding(path.node.name);
  if (!binding || seen.has(binding) || !binding.constant) return undefined;
  seen.add(binding);
  const declared = binding.path;
  if (declared.isImportSpecifier() || declared.isImportNamespaceSpecifier()) {
    const statement = declared.parentPath;
    if (!statement.isImportDeclaration() || statement.node.importKind === 'type') return undefined;
    if (declared.isImportNamespaceSpecifier()) return { source: statement.node.source.value };
    if (declared.node.importKind === 'type') return undefined;
    const imported = declared.node.imported;
    return { source: statement.node.source.value, name: imported.type === 'Identifier' ? imported.name : imported.value };
  }
  if (!declared.isVariableDeclarator() || !declared.node.init) return undefined;
  const imported = reference(declared.get('init') as NodePath, seen);
  if (declared.node.id.type === 'Identifier') return imported;
  if (declared.node.id.type !== 'ObjectPattern' || !imported || imported.name) return undefined;
  for (const property of declared.node.id.properties) {
    if (property.type !== 'ObjectProperty' || property.value.type !== 'Identifier' || property.value.name !== path.node.name) continue;
    const key = property.key;
    const name = !property.computed && key.type === 'Identifier' ? key.name : key.type === 'StringLiteral' ? key.value : undefined;
    if (name) return { source: imported.source, name };
  }
  return undefined;
}

/** Does this value reach a value export, possibly through local bindings? */
function reachesExport(path: NodePath, seen = new Set<Binding>()): boolean {
  for (let parent: NodePath | null = path; parent && !parent.isProgram(); parent = parent.parentPath) {
    if (parent.isExportNamedDeclaration()) return parent.node.exportKind !== 'type';
    if (parent.isExportDefaultDeclaration()) return true;
    if (parent.isExportSpecifier() && parent.node.exportKind === 'type') return false;
    if (parent.node.type.startsWith('TS') && !parent.isTSAsExpression() && !parent.isTSSatisfiesExpression() && !parent.isTSNonNullExpression() && !parent.isTSTypeAssertion()) return false;
    if (parent.isAssignmentExpression()) {
      const left = parent.get('left');
      if (left.isMemberExpression()) {
        const { object, property, computed } = left.node;
        const key = !computed && property.type === 'Identifier' ? property.name : property.type === 'StringLiteral' ? property.value : undefined;
        if (object.type === 'Identifier' && object.name === 'module' && key === 'exports' && !left.scope.getBinding('module')) return true;
        if (object.type === 'Identifier' && object.name === 'exports' && !left.scope.getBinding('exports')) return true;
        if (object.type === 'MemberExpression' && object.object.type === 'Identifier' && object.object.name === 'module' && !left.scope.getBinding('module') && !object.computed && object.property.type === 'Identifier' && object.property.name === 'exports') return true;
      }
    }
    if (!parent.isVariableDeclarator() && !parent.isFunctionDeclaration() && !parent.isClassDeclaration() && !parent.isAssignmentExpression()) continue;
    const identifiers = parent.isAssignmentExpression() ? parent.get('left').getBindingIdentifiers() : parent.getBindingIdentifiers();
    for (const name of Object.keys(identifiers)) {
      const binding = parent.scope.getBinding(name);
      if (!binding || !binding.scope.path.isProgram() || seen.has(binding)) continue;
      seen.add(binding);
      if (binding.referencePaths.some((ref) => reachesExport(ref, seen))) return true;
    }
  }
  return false;
}
