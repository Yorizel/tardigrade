import { parseSync } from "oxc-parser"
import { Array as Arr, Effect, FileSystem, Option, Path, Record as Rec, Schema, String as Str } from "effect"

class SourceClosureRejected extends Schema.TaggedError<SourceClosureRejected, { readonly brand: unique symbol }>()("SourceClosureRejected", { reason: Schema.String }) {}
const UnknownRecord = Schema.Record(Schema.String, Schema.Unknown)

const importsIn = Effect.fn("publication.sourceImports")(function* (source: string, file: string) {
  const parsed = parseSync(file, source)
  if (parsed.errors.length > 0) return yield* SourceClosureRejected.make({ reason: `Cannot parse ${file}: ${Arr.join(Arr.map(parsed.errors, error => error.message), ", ")}` })
  const imports: string[] = []
  const visit = Effect.fn("publication.visitSource")(function* (value: unknown): Effect.fn.Return<void, SourceClosureRejected> {
    if (value === null || typeof value !== "object") return undefined
    if (Arr.isArray(value)) {
      for (const child of value) yield* visit(child)
      return undefined
    }
    if (!Schema.is(UnknownRecord)(value)) return undefined
    if (Arr.contains(["ImportDeclaration", "ExportNamedDeclaration", "ExportAllDeclaration", "ImportExpression", "TSImportType"], String(value.type))) {
      const moduleSource = value.source
      if (Schema.is(UnknownRecord)(moduleSource) && typeof moduleSource.value === "string") imports.push(moduleSource.value)
      else if (value.type === "ImportExpression" || value.type === "TSImportType") return yield* SourceClosureRejected.make({ reason: `Non-literal import in ${file}` })
    }
    if (value.type === "TSExternalModuleReference") {
      const expression = value.expression
      if (Schema.is(UnknownRecord)(expression) && typeof expression.value === "string") imports.push(expression.value)
      else return yield* SourceClosureRejected.make({ reason: `Non-literal import in ${file}` })
    }
    if (value.type === "CallExpression" && Schema.is(UnknownRecord)(value.callee) && value.callee.name === "require") {
      const argument = Arr.isArray(value.arguments) ? value.arguments[0] : undefined
      if (Schema.is(UnknownRecord)(argument) && typeof argument.value === "string") imports.push(argument.value)
      else return yield* SourceClosureRejected.make({ reason: `Non-literal require in ${file}` })
    }
    for (const child of Rec.values(value)) yield* visit(child)
    return undefined
  })
  yield* visit(parsed.program)
  return imports
})

export const pruneToSourceClosure = Effect.fn("publication.pruneToSourceClosure")(function* (sourceRoot: string, entries: readonly string[]) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const root = yield* fs.realPath(sourceRoot)
  const pending = Arr.map(entries, entry => path.resolve(root, entry))
  const visited = new Set<string>()
  const external = new Set<string>()
  while (pending.length > 0) {
    const candidate = pending.pop()
    if (candidate === undefined) break
    if (candidate !== root && !Str.startsWith(`${root}${path.sep}`)(candidate)) return yield* SourceClosureRejected.make({ reason: `Source import escapes package: ${candidate}` })
    const resolved = yield* fs.realPath(candidate)
    if (!Str.startsWith(`${root}${path.sep}`)(resolved)) return yield* SourceClosureRejected.make({ reason: `Unresolved or escaping source import: ${candidate}` })
    if (visited.has(resolved)) continue
    visited.add(resolved)
    const contents = yield* fs.readFileString(resolved)
    for (const specifier of yield* importsIn(contents, resolved)) {
      if (!Str.startsWith(".")(specifier)) {
        if (path.isAbsolute(specifier)) return yield* SourceClosureRejected.make({ reason: `Absolute source import in ${resolved}: ${specifier}` })
        external.add(specifier)
        continue
      }
      const base = path.resolve(path.dirname(resolved), specifier)
      const candidates = path.extname(base) === ".js" ? [`${Str.slice(0, -3)(base)}.ts`, base] : [base, `${base}.ts`, `${base}.tsx`, `${base}.d.ts`, path.resolve(base, "index.ts")]
      const found = yield* Effect.forEach(candidates, Effect.fn("publication.findSource")(function* (possible) {
        if (!(yield* fs.exists(possible))) return Option.none<string>()
        const info = yield* fs.stat(possible)
        return info.type === "File" ? Option.some(possible) : Option.none<string>()
      }))
      const next = Arr.findFirst(found, Option.isSome)
      if (Option.isNone(next)) return yield* SourceClosureRejected.make({ reason: `Unresolved source import ${specifier} in ${path.relative(root, resolved)}` })
      pending.push(next.value.value)
    }
  }
  const files = yield* fs.glob("**/*", { root, dot: true })
  for (const file of files) {
    const absolute = path.resolve(root, file)
    const info = yield* fs.stat(absolute)
    if (info.type === "File" && !visited.has(absolute)) yield* fs.remove(absolute)
  }
  return [...external]
})
