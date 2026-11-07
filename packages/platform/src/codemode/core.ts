import { parse, tokenizer, type AnyNode, type ArrowFunctionExpression, type FunctionExpression, type CallExpression, type Program } from "acorn"
import { CodeInput, CodeLimits, CodeSettlementSequence, DEFAULT_CODE_LIMITS, HostCallInput, MAX_CODE_DEPTH, PackageCatalog } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Array as Arr, Cause, Clock, Effect, Function as Fn, Option, Queue, Record as Rec, Result as EffectResult, Schema, Scope, String as Str } from "effect"

export const maxGuestDepth = MAX_CODE_DEPTH
export const Budgets = Schema.Struct({ sourceChars: CodeLimits.fields.maxCodeChars, steps: CodeLimits.fields.maxSteps, calls: CodeLimits.fields.maxCalls, callInputChars: CodeLimits.fields.maxCallInputChars, resultChars: CodeLimits.fields.maxResultChars, logs: CodeLimits.fields.maxLogs, logChars: CodeLimits.fields.maxLogChars, deadlineMs: CodeLimits.fields.maxSegmentMs, astNodes: CodeLimits.fields.maxAstNodes, astDepth: CodeLimits.fields.maxAstDepth, jsonNodes: CodeLimits.fields.maxJsonNodes, jsonDepth: CodeLimits.fields.maxJsonDepth, callDepth: CodeLimits.fields.maxCallDepth, executionDepth: CodeLimits.fields.maxExecutionDepth, ambientChars: CodeLimits.fields.maxAmbientChars })
export type Budgets = typeof Budgets.Type
export const defaultBudgets: Budgets = { sourceChars: DEFAULT_CODE_LIMITS.maxCodeChars, steps: DEFAULT_CODE_LIMITS.maxSteps, calls: DEFAULT_CODE_LIMITS.maxCalls, callInputChars: DEFAULT_CODE_LIMITS.maxCallInputChars, resultChars: DEFAULT_CODE_LIMITS.maxResultChars, logs: DEFAULT_CODE_LIMITS.maxLogs, logChars: DEFAULT_CODE_LIMITS.maxLogChars, deadlineMs: DEFAULT_CODE_LIMITS.maxSegmentMs, astNodes: DEFAULT_CODE_LIMITS.maxAstNodes, astDepth: DEFAULT_CODE_LIMITS.maxAstDepth, jsonNodes: DEFAULT_CODE_LIMITS.maxJsonNodes, jsonDepth: DEFAULT_CODE_LIMITS.maxJsonDepth, callDepth: DEFAULT_CODE_LIMITS.maxCallDepth, executionDepth: DEFAULT_CODE_LIMITS.maxExecutionDepth, ambientChars: DEFAULT_CODE_LIMITS.maxAmbientChars }
export const Input = Schema.Struct({ source: Schema.String, origin: Schema.String, budgets: Budgets, ambient: CodeInput.fields.ambient })
export type Input = typeof Input.Type
export const Request = Schema.Struct({ ordinal: HostCallInput.fields.ordinal, package: HostCallInput.fields.package, method: HostCallInput.fields.method, origin: Schema.Json, input: Schema.optionalKey(Schema.Json) })
export type Request = typeof Request.Type
export const Outcome = Schema.Union([Schema.TaggedStruct("Success", { value: Schema.optionalKey(Schema.Json), logs: Schema.Array(Schema.String), returnedCallOrdinal: Schema.optionalKey(HostCallInput.fields.ordinal) }), Schema.TaggedStruct("Failure", { reason: Schema.String }), Schema.TaggedStruct("ToolFailure", { reason: Schema.String }), Schema.TaggedStruct("GuestFailure", { reason: Schema.String })])
export type Outcome = typeof Outcome.Type
const SettlementOutcome = Schema.Union([Schema.TaggedStruct("Json", { value: Schema.Json }), Schema.TaggedStruct("Void", {}), Schema.TaggedStruct("Failed", { reason: Schema.String })])
const Settlement = Schema.Struct({ request: Request, sequence: CodeSettlementSequence, outcome: SettlementOutcome })
export const Model = Schema.Struct({ input: Input, catalog: PackageCatalog, settlements: Schema.Array(Settlement), pending: Schema.Array(Request), logs: Schema.Array(Schema.String), outcome: Schema.optionalKey(Outcome) })
export type Model = typeof Model.Type
export const Message = Schema.Union([Schema.TaggedStruct("Start", {}), Schema.TaggedStruct("Settled", { ordinal: HostCallInput.fields.ordinal, sequence: CodeSettlementSequence, value: Schema.optionalKey(Schema.String) }), Schema.TaggedStruct("Failed", { ordinal: HostCallInput.fields.ordinal, sequence: CodeSettlementSequence, reason: Schema.String })])
export type Message = typeof Message.Type

type Result<A> = { readonly tag: "ok"; readonly value: A } | { readonly tag: "error"; readonly reason: string } | { readonly tag: "pending"; readonly requests: readonly Request[]; readonly waiting?: number; readonly waitingCalls?: readonly number[]; readonly waitingJob?: GuestJob; readonly waitingJobs?: readonly GuestJob[]; readonly microtask?: boolean; readonly wake?: boolean; readonly logs?: readonly string[] } | { readonly tag: "guest"; readonly reason: string; readonly kind: "Tool" | "Guest"; readonly logs?: readonly string[]; readonly beforePromise?: boolean }
const ok = <A>(value: A): Result<A> => ({ tag: "ok", value })
const error = (reason: string): Result<never> => ({ tag: "error", reason })
const guestError = (reason: string): Result<never> => ({ tag: "guest", kind: "Guest", reason })
const truthy: (value: unknown) => boolean = Boolean
const forbidden = new Set(["__proto__", "prototype", "constructor"])

function batchCall(node: AnyNode): "all" | "race" | undefined {
  if (node.type !== "MemberExpression" || node.computed || node.optional || node.object.type !== "Identifier" || node.object.name !== "Promise" || node.property.type !== "Identifier") return undefined
  if (node.property.name === "all" || node.property.name === "race") return node.property.name
  return undefined
}

function resolveCall(node: AnyNode): boolean {
  return node.type === "MemberExpression" && !node.computed && !node.optional && node.object.type === "Identifier" && node.object.name === "Promise" && node.property.type === "Identifier" && node.property.name === "resolve"
}

function thenBatch(node: AnyNode): { readonly source: AnyNode; readonly callback: AnyNode; readonly kind: "then" | "catch" } | undefined {
  if (node.type !== "CallExpression" || node.optional || node.arguments.length !== 1 || node.callee.type !== "MemberExpression" || node.callee.computed || node.callee.optional || node.callee.property.type !== "Identifier" || (node.callee.property.name !== "then" && node.callee.property.name !== "catch")) return undefined
  const callback = node.arguments[0]
  if (callback?.type !== "ArrowFunctionExpression" && callback?.type !== "FunctionExpression" && callback?.type !== "Identifier") return undefined
  let source: AnyNode = node.callee.object
  let depth = 0
  while (source.type === "CallExpression") {
    if (batchCall(source.callee) !== undefined || resolveCall(source.callee) || toolPath(source.callee) !== undefined) break
    if (source.callee.type !== "MemberExpression" || source.callee.computed || source.callee.optional || source.callee.property.type !== "Identifier" || (source.callee.property.name !== "then" && source.callee.property.name !== "catch") || source.arguments.length !== 1) return undefined
    const handler = source.arguments[0]
    if (handler?.type !== "ArrowFunctionExpression" && handler?.type !== "FunctionExpression" && handler?.type !== "Identifier") return undefined
    if (++depth > maxGuestDepth) return undefined
    source = source.callee.object
  }
  if (source.type !== "Identifier" && source.type !== "CallExpression") return undefined
  return { source: node.callee.object, callback, kind: node.callee.property.name }
}

function repeatCall(node: AnyNode): boolean {
  return node.type === "MemberExpression" && !node.computed && !node.optional && node.object.type === "Literal" && typeof node.object.value === "string" && node.property.type === "Identifier" && node.property.name === "repeat"
}

function consoleCall(node: AnyNode): boolean {
  return node.type === "MemberExpression" && !node.computed && !node.optional && node.object.type === "Identifier" && node.object.name === "console" && node.property.type === "Identifier" && node.property.name === "log"
}

function quoteLogString(value: string): string {
  const chunks = ['"']
  const digits = "0123456789abcdef"
  for (let index = 0; index < value.length; index++) {
    const code = Option.getOrElse(Str.charCodeAt(value, index), () => 0)
    if (code === 34) { chunks.push('\\"'); continue }
    if (code === 92) { chunks.push('\\\\'); continue }
    if (code === 8) { chunks.push('\\b'); continue }
    if (code === 9) { chunks.push('\\t'); continue }
    if (code === 10) { chunks.push('\\n'); continue }
    if (code === 12) { chunks.push('\\f'); continue }
    if (code === 13) { chunks.push('\\r'); continue }
    const next = Option.getOrElse(Str.charCodeAt(value, index + 1), () => 0)
    if (code >= 0xd800 && code <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
      chunks.push(value[index]!, value[index + 1]!)
      index++
      continue
    }
    if (code < 32 || code >= 0xd800 && code <= 0xdfff) {
      chunks.push('\\u', digits[(code >>> 12) & 15]!, digits[(code >>> 8) & 15]!, digits[(code >>> 4) & 15]!, digits[code & 15]!)
      continue
    }
    chunks.push(value[index]!)
  }
  chunks.push('"')
  return Arr.join(chunks, "")
}

function isJsonArray(value: Schema.Json): value is readonly Schema.Json[] {
  return Arr.isArray(value)
}

function logJson(value: Schema.Json): string {
  if (value === null) return "null"
  if (typeof value === "string") return quoteLogString(value)
  if (typeof value === "number" || typeof value === "boolean") return String(value)
  if (isJsonArray(value)) return `[${Arr.join(Arr.map(value, logJson), ",")}]`
  return `{${Arr.join(Arr.map(Rec.toEntries(value), ([key, item]) => `${quoteLogString(key)}:${logJson(item)}`), ",")}}`
}

function dateConstructor(node: AnyNode): boolean {
  if (node.type === "Identifier") return node.name === "Date"
  return node.type === "MemberExpression" && !node.computed && !node.optional && node.property.type === "Identifier" && node.property.name === "constructor" && node.object.type === "MemberExpression" && !node.object.computed && !node.object.optional && node.object.property.type === "Identifier" && node.object.property.name === "prototype" && node.object.object.type === "Identifier" && node.object.object.name === "Date"
}

function ambientCall(node: AnyNode): "clock" | "random" | undefined {
  if (node.type !== "MemberExpression" || node.computed || node.optional || node.object.type !== "Identifier" || node.property.type !== "Identifier") return undefined
  if (node.object.name === "Date" && node.property.name === "now") return "clock"
  if (node.object.name === "Math" && node.property.name === "random") return "random"
  return undefined
}

function toolPath(node: AnyNode): string | undefined {
  const segments: string[] = []
  let current: AnyNode = node
  while (current.type === "MemberExpression") {
    if (current.computed || current.optional || current.property.type !== "Identifier" || forbidden.has(current.property.name)) return undefined
    segments.unshift(current.property.name)
    current = current.object
  }
  if (current.type !== "Identifier" || segments.length === 0 || forbidden.has(current.name)) return undefined
  if (current.name !== "tools") segments.unshift(current.name)
  return Arr.join(segments, ".")
}

function copyJson(value: unknown, budgets: Budgets): Result<Schema.Json> {
  let nodes = 0
  let chars = 0
  const seen = new Set<object>()
  const add = (n: number) => { chars += n; return chars <= budgets.resultChars }
  const stringSize = (text: string): boolean => {
    if (!add(2)) return false
    for (let i = 0; i < text.length; i++) {
      const code = Option.getOrElse(Str.charCodeAt(text, i), () => 0)
      const size = code === 34 || code === 92 || code === 8 || code === 9 || code === 10 || code === 12 || code === 13 ? 2 : code < 32 || code >= 0xd800 && code <= 0xdfff && !(code <= 0xdbff && i + 1 < text.length && Option.getOrElse(Str.charCodeAt(text, i + 1), () => 0) >= 0xdc00 && Option.getOrElse(Str.charCodeAt(text, i + 1), () => 0) <= 0xdfff) && !(code >= 0xdc00 && i > 0 && Option.getOrElse(Str.charCodeAt(text, i - 1), () => 0) >= 0xd800 && Option.getOrElse(Str.charCodeAt(text, i - 1), () => 0) <= 0xdbff) ? 6 : 1
      if (!add(size)) return false
    }
    return true
  }
  const visit = (item: unknown, depth: number): Result<Schema.Json> => {
    if (++nodes > budgets.jsonNodes) return error("JsonNodeBudgetExceeded")
    if (depth > budgets.jsonDepth) return error("JsonDepthBudgetExceeded")
    if (item === null) return add(4) ? ok(null) : error("ResultBudgetExceeded")
    if (typeof item === "string") return stringSize(item) ? ok(item) : error("ResultBudgetExceeded")
    if (typeof item === "boolean") return add(item ? 4 : 5) ? ok(item) : error("ResultBudgetExceeded")
    if (typeof item === "number" && Number.isFinite(item)) return add(String(item).length) ? ok(item) : error("ResultBudgetExceeded")
    if (typeof item !== "object" || seen.has(item)) return error("InvalidJson")
    const proto = Object.getPrototypeOf(item)
    if (proto !== Object.prototype && proto !== null && proto !== Array.prototype) return error("InvalidJson")
    seen.add(item)
    if (Arr.isArray(item)) {
      if (item.length > budgets.jsonNodes - nodes || !add(2 + Math.max(0, item.length - 1))) return error("JsonNodeBudgetExceeded")
      const keys = Reflect.ownKeys(item)
      if (keys.length !== item.length + 1) return error("InvalidJson")
      const array: Schema.Json[] = []
      for (let i = 0; i < item.length; i++) {
        const descriptor = Object.getOwnPropertyDescriptor(item, String(i))
        if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) return error("InvalidJson")
        const child = visit(descriptor.value, depth + 1)
        if (child.tag !== "ok") return child
        array.push(child.value)
      }
      seen.delete(item)
      return ok(array)
    }
    const keys = Reflect.ownKeys(item)
    if (keys.length > budgets.jsonNodes - nodes || !add(2 + Math.max(0, keys.length - 1))) return error("JsonNodeBudgetExceeded")
    const object: Record<string, Schema.Json> = Object.create(null)
    for (const key of keys) {
      if (typeof key !== "string" || forbidden.has(key)) return error("InvalidJson")
      const descriptor = Object.getOwnPropertyDescriptor(item, key)
      if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) return error("InvalidJson")
      if (!stringSize(key) || !add(1)) return error("ResultBudgetExceeded")
      const child = visit(descriptor.value, depth + 1)
      if (child.tag !== "ok") return child
      object[key] = child.value
    }
    seen.delete(item)
    return ok(object)
  }
  return visit(value, 0)
}

function decodeHostJson(text: unknown, budgets: Budgets): Result<Schema.Json> {
  if (typeof text !== "string") return error("InvalidJsonText")
  if (text.length > budgets.resultChars) return error("ResultBudgetExceeded")
  let depth = 0
  let nodes = 0
  let inString = false
  let escaped = false
  let primitive = false
  for (const character of text) {
    if (inString) {
      if (escaped) escaped = false
      else if (character === "\\") escaped = true
      else if (character === '"') inString = false
      continue
    }
    if (character === '"') { inString = true; primitive = false; nodes++ }
    else if (character === "{" || character === "[") { depth++; nodes++; primitive = false; if (depth > budgets.jsonDepth) return error("JsonDepthBudgetExceeded") }
    else if (character === "}" || character === "]") { depth--; primitive = false }
    else if (character === "," || character === ":" || /\s/.test(character)) primitive = false
    else if (!primitive) { nodes++; primitive = true }
    if (nodes > budgets.jsonNodes) return error("JsonNodeBudgetExceeded")
  }
  const decoded = Schema.decodeResult(Schema.fromJsonString(Schema.Json))(text)
  if (decoded._tag === "Failure") return error("InvalidJson")
  return copyJson(decoded.success, budgets)
}

function equalJson(left: Schema.Json, right: Schema.Json): boolean {
  if (left === right) return true
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object" || Arr.isArray(left) !== Arr.isArray(right)) return false
  const leftKeys = Reflect.ownKeys(left)
  const rightKeys = Reflect.ownKeys(right)
  if (leftKeys.length !== rightKeys.length) return false
  for (const key of leftKeys) {
    const a = Object.getOwnPropertyDescriptor(left, key)
    const b = Object.getOwnPropertyDescriptor(right, key)
    if (a === undefined || b === undefined || !("value" in a) || !("value" in b) || !equalJson(a.value, b.value)) return false
  }
  return true
}

function sameRequest(left: Request, right: Request, budgets: Budgets): boolean {
  const keys = Reflect.ownKeys(left)
  const expectedKeys = Reflect.ownKeys(right)
  if (keys.length !== expectedKeys.length || Arr.some(keys, key => typeof key !== "string" || !expectedKeys.includes(key))) return false
  for (const key of expectedKeys) {
    const actual = Object.getOwnPropertyDescriptor(left, key)
    const wanted = Object.getOwnPropertyDescriptor(right, key)
    if (actual === undefined || wanted === undefined || !("value" in actual) || !("value" in wanted)) return false
    if (key === "ordinal" || key === "package" || key === "method") { if (actual.value !== wanted.value) return false; continue }
    const copied = copyJson(actual.value, key === "input" ? { ...budgets, resultChars: budgets.callInputChars } : key === "origin" ? { ...budgets, resultChars: budgets.ambientChars, jsonNodes: budgets.ambientChars, jsonDepth: maxGuestDepth } : budgets)
    if (copied.tag !== "ok" || !equalJson(copied.value, wanted.value)) return false
  }
  return true
}

function syntax(source: string, budgets: Budgets, paths: ReadonlyMap<string, unknown>, reserved: ReadonlySet<string>): Result<Program> {
  if (source.length > budgets.sourceChars) return error("SourceBudgetExceeded")
  try {
    const tokens = tokenizer(source, { ecmaVersion: 2022, allowAwaitOutsideFunction: true, allowReturnOutsideFunction: true })
    let count = 0
    let depth = 0
    for (;;) {
      const token = tokens.getToken()
      if (token.type.label === "eof") break
      if (++count > budgets.astNodes * 4) return error("AstNodeBudgetExceeded")
      if (["(", "[", "{"].includes(token.type.label) && ++depth > budgets.astDepth) return error("AstDepthBudgetExceeded")
      if ([")", "]", "}"].includes(token.type.label)) depth--
    }
    const ast = parse(source, { ecmaVersion: 2022, allowReturnOutsideFunction: true, allowAwaitOutsideFunction: true })
    let nodes = 0
    const bindings = new Set<string>()
    const writeRoots: string[] = []
    const visit = (node: AnyNode, level: number, parent?: AnyNode, batch = false, then = false): Result<null> => {
      if (++nodes > budgets.astNodes) return error("AstNodeBudgetExceeded")
      if (level > budgets.astDepth) return error("AstDepthBudgetExceeded")
      const children: AnyNode[] = []
      switch (node.type) {
        case "Program": case "BlockStatement": children.push(...node.body); break
        case "VariableDeclaration": if (node.kind !== "const" && node.kind !== "let") return error("UnsupportedSyntax"); children.push(...node.declarations); break
        case "VariableDeclarator": if (node.id.type !== "Identifier" || node.init == null || reserved.has(node.id.name)) return error("UnsupportedSyntax"); bindings.add(node.id.name); children.push(node.id, node.init); break
        case "Identifier": break
        case "Literal": if (node.regex !== undefined || node.bigint !== undefined || node.value === undefined || typeof node.value === "number" && !Number.isFinite(node.value) || node.value instanceof RegExp) return error("UnsupportedSyntax"); break
        case "ObjectExpression": {
          const keys = new Set<string>()
          for (const property of node.properties) {
            if (property.type !== "Property" || property.computed || property.key.type !== "Identifier" && property.key.type !== "Literal") return error("UnsupportedSyntax")
            const key = property.key.type === "Identifier" ? property.key.name : String(property.key.value)
            if (forbidden.has(key) || keys.has(key)) return error("InvalidObjectKey")
            keys.add(key)
          }
          children.push(...node.properties)
          break
        }
        case "Property": if (node.kind !== "init" || node.method || node.computed || !["Identifier", "Literal"].includes(node.key.type)) return error("UnsupportedSyntax"); if (node.key.type === "Identifier" && forbidden.has(node.key.name) || node.key.type === "Literal" && forbidden.has(String(node.key.value))) return error("InvalidObjectKey"); children.push(node.key, node.value); break
        case "ArrayExpression": if (Arr.some(node.elements, element => element === null)) return error("UnsupportedSyntax"); for (const element of node.elements) if (element !== null) children.push(element); break
        case "MemberExpression": if (node.optional || node.property.type === "PrivateIdentifier" || !node.computed && node.property.type !== "Identifier") return error("UnsupportedSyntax"); if (node.computed && node.property.type === "Literal" && typeof node.property.value === "string" && forbidden.has(node.property.value)) return error("InvalidMember"); if (!node.computed && node.property.type === "Identifier" && forbidden.has(node.property.name) && !dateConstructor(node) && !(node.property.name === "prototype" && parent !== undefined && dateConstructor(parent))) return error("InvalidMember"); children.push(node.object, node.property); break
        case "ArrowFunctionExpression": case "FunctionExpression": if (node.async && !then && parent?.type !== "VariableDeclarator" || node.generator || node.type === "FunctionExpression" && node.id != null || Arr.some(node.params, param => param.type !== "Identifier" || reserved.has(param.name))) return error("UnsupportedSyntax"); for (const param of node.params) if (param.type === "Identifier") bindings.add(param.name); children.push(...node.params, node.body); break
        case "CallExpression": {
          if (node.optional) return error("UnsupportedSyntax")
          const continuation = thenBatch(node)
          const combine = batchCall(node.callee)
          if (continuation !== undefined) {
            if ((continuation.callback.type === "ArrowFunctionExpression" || continuation.callback.type === "FunctionExpression") && continuation.callback.params.length > 1) return error("UnsupportedSyntax")
          } else if (consoleCall(node.callee)) {
            if (node.arguments.length > budgets.astNodes) return error("AstNodeBudgetExceeded")
          } else if (repeatCall(node.callee)) {
            if (node.arguments.length !== 1) return error("UnsupportedSyntax")
          } else if (resolveCall(node.callee)) {
            if (node.arguments.length > 1) return error("UnsupportedSyntax")
          } else if (combine !== undefined) {
            const argument = node.arguments[0]
            if (node.arguments.length !== 1 || argument === undefined) return error("UnsupportedSyntax")
            if (argument.type !== "Identifier" && (argument.type !== "ArrayExpression" || Arr.some(argument.elements, element => element === null || element.type !== "Identifier" && element.type !== "Literal" && element.type !== "CallExpression"))) return error("UnsupportedSyntax")
          } else if (node.callee.type !== "Identifier") {
            const ambient = ambientCall(node.callee)
            if (ambient !== undefined && node.arguments.length !== 0) return error("UnsupportedSyntax")
            if (ambient === undefined) {
              const path = toolPath(node.callee)
              if (path === undefined) return error("UnsupportedSyntax")
              if (!paths.has(path)) return error("UnknownTool")
              if (node.arguments.length > 1) return error("InvalidCallInput")
            }
          }
          if (continuation === undefined && combine === undefined && !consoleCall(node.callee) && !repeatCall(node.callee) && ambientCall(node.callee) === undefined && !resolveCall(node.callee) && node.callee.type !== "Identifier") {
            const path = toolPath(node.callee)
            if (path === undefined || !paths.has(path)) return error("UnknownTool")
          }
          children.push(node.callee, ...node.arguments)
          break
        }
        case "NewExpression": if (!dateConstructor(node.callee) || node.arguments.length > 1) return error("UnsupportedSyntax"); children.push(node.callee, ...node.arguments); break
        case "AssignmentExpression": if (node.operator !== "=" || node.left.type !== "Identifier" && (node.left.type !== "MemberExpression" || node.left.optional || node.left.property.type === "PrivateIdentifier" || !node.left.computed && node.left.property.type !== "Identifier")) return error("UnsupportedSyntax"); if (node.left.type === "MemberExpression") { if (!node.left.computed && node.left.property.type === "Identifier" && forbidden.has(node.left.property.name)) return error("InvalidMember"); let root: AnyNode = node.left.object; while (root.type === "MemberExpression") root = root.object; if (root.type !== "Identifier") return error("UnsupportedSyntax"); writeRoots.push(root.name) } children.push(node.left, node.right); break
        case "UpdateExpression": if (!["++", "--"].includes(node.operator) || node.argument.type !== "Identifier") return error("UnsupportedSyntax"); children.push(node.argument); break
        case "WhileStatement": children.push(node.test, node.body); break
        case "TryStatement": if (node.handler == null || node.finalizer != null) return error("UnsupportedSyntax"); children.push(node.block, node.handler); break
        case "CatchClause": if (node.param != null && (node.param.type !== "Identifier" || reserved.has(node.param.name))) return error("UnsupportedSyntax"); if (node.param?.type === "Identifier") bindings.add(node.param.name); if (node.param != null) children.push(node.param); children.push(node.body); break
        case "AwaitExpression": children.push(node.argument); break
        case "ReturnStatement": if (node.argument != null) children.push(node.argument); break
        case "ExpressionStatement": children.push(node.expression); break
        case "IfStatement": children.push(node.test, node.consequent); if (node.alternate != null) children.push(node.alternate); break
        case "BinaryExpression": if (!["===", "!==", "<", ">", "<=", ">=", "+", "-", "*", "/"].includes(node.operator)) return error("UnsupportedSyntax"); children.push(node.left, node.right); break
        case "UnaryExpression": if (!["!", "-", "+", "typeof"].includes(node.operator)) return error("UnsupportedSyntax"); children.push(node.argument); break
        default: return error("UnsupportedSyntax")
      }
      for (const child of children) { const batchChild = node.type === "CallExpression" && batchCall(node.callee) !== undefined && child === node.arguments[0] || batch && node.type === "ArrayExpression"; const thenChild = node.type === "CallExpression" && thenBatch(node) !== undefined && (child === node.callee || child === node.arguments[0]) || then && node.type === "MemberExpression" && child === node.object; const result = visit(child, level + 1, node, batchChild, thenChild); if (result.tag !== "ok") return result }
      return ok(null)
    }
    const checked = visit(ast, 0)
    if (checked.tag === "ok" && Arr.some(writeRoots, root => !bindings.has(root))) return error("UnsupportedSyntax")
    return checked.tag === "ok" ? ok(ast) : checked
  } catch { return error("InvalidSyntax") }
}

const dateBrand = Symbol("GuestDate")
type GuestDate = { readonly [dateBrand]: true; readonly time: number }
const promiseBrand = Symbol("GuestPromise")
type GuestJob = { readonly [promiseBrand]: true; readonly kind: "Job"; readonly id: number; readonly root: boolean; readonly frame: Generator<Result<Execution>, Result<Execution>, unknown>; readonly dependencies: Set<GuestPromise>; readonly subscriptions: Map<string, number>; step: IteratorResult<Result<Execution>, Result<Execution>>; scopes: Map<string, Binding>[]; mutableScopes: Set<string>[]; callDepth: number; executionDepth: number; adoptionDepth: number; completionOrder: number | undefined; completionSequence: number | undefined; observed: boolean; handled: boolean }
type GuestPromise = { readonly [promiseBrand]: true; readonly kind: "Tool"; readonly request: Request } | { readonly [promiseBrand]: true; readonly kind: "Resolved"; readonly value: Value } | { readonly [promiseBrand]: true; readonly kind: "Rejected"; readonly reason: string; readonly errorKind: "Tool" | "Guest" } | { readonly [promiseBrand]: true; readonly kind: "Adopted"; readonly source: GuestPromise } | GuestJob
const closureBrand = Symbol("GuestClosure")
const uninitialized = Symbol("UninitializedBinding")
type Binding = Value | typeof uninitialized
type Closure = { readonly [closureBrand]: true; readonly node: ArrowFunctionExpression | FunctionExpression; readonly scopes: Map<string, Binding>[]; readonly mutableScopes: Set<string>[] }
type Value = Schema.Json | Closure | GuestPromise | GuestDate | readonly Value[] | { readonly [key: string]: Value } | undefined
function isValueArray(value: Value): value is readonly Value[] {
  return Arr.isArray(value)
}
function isDate(value: Value): value is GuestDate {
  return value !== null && typeof value === "object" && dateBrand in value
}
function isPromise(value: Value): value is GuestPromise {
  return value !== null && typeof value === "object" && promiseBrand in value
}
function isClosure(value: Value): value is Closure {
  return value !== null && typeof value === "object" && closureBrand in value
}
type Execution = { readonly returned: boolean; readonly value: Value }
const result = (value?: Value, returned = false): Result<Execution> => ok({ value, returned })
function evaluate(model: Model): Result<Outcome> {
  const { input } = model
  if (!Schema.is(Budgets)(input.budgets)) return error("InvalidBudget")
  if (!Schema.is(CodeInput.fields.ambient)(input.ambient)) return error("InvalidAmbient")
  if (input.ambient.seed.length > input.budgets.ambientChars) return error("AmbientBudgetExceeded")
  const paths = new Map<string, { readonly package: string; readonly method: string }>()
  const reserved = new Set(["Date", "Math", "Promise", "console", "tools"])
  for (const [pkg, methods] of Rec.toEntries(model.catalog)) {
    if (pkg !== "tools") reserved.add(Str.split(pkg, ".")[0])
    for (const method of methods) {
      const segments = [...(pkg === "tools" ? [] : Str.split(pkg, ".")), ...Str.split(method, ".")]
      if (segments[0] === "Date" || segments[0] === "Math" || segments[0] === "Promise" || segments[0] === "console") return error("InvalidToolCatalog")
      if (Arr.some(segments, part => part.length === 0 || forbidden.has(part) || !/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(part))) return error("InvalidToolCatalog")
      const path = Arr.join(segments, ".")
      if (Arr.some([...paths.keys()], existing => existing === path || Str.startsWith(`${path}.`)(existing) || Str.startsWith(`${existing}.`)(path))) return error("InvalidToolCatalog")
      paths.set(path, { package: pkg, method })
    }
  }
  const origin = decodeHostJson(input.origin, { ...input.budgets, resultChars: input.budgets.ambientChars, jsonNodes: input.budgets.ambientChars, jsonDepth: maxGuestDepth })
  if (origin.tag !== "ok") return origin
  const parsed = syntax(input.source, input.budgets, paths, reserved)
  if (parsed.tag !== "ok") return parsed
  const program = parsed.value
  const provenance = new WeakMap<object, typeof Settlement.Type>()
  const guestValues = new Map<number, Schema.Json>()
  const guestValue = (settlement: typeof Settlement.Type): Result<Schema.Json> => {
    if (settlement.outcome._tag !== "Json") return error("InvalidModel")
    if (guestValues.has(settlement.request.ordinal)) return ok(guestValues.get(settlement.request.ordinal)!)
    const copied = copyJson(settlement.outcome.value, input.budgets)
    if (copied.tag !== "ok") return copied
    guestValues.set(settlement.request.ordinal, copied.value)
    if (copied.value !== null && typeof copied.value === "object") provenance.set(copied.value, settlement)
    return copied
  }
  let randomState = 2166136261
  for (const code of input.ambient.seed) {
    randomState = Math.imul(randomState ^ Option.getOrElse(Str.codePointAt(code, 0), () => 0), 16777619)
  }
  if (randomState === 0) randomState = 0x6d2b79f5
  const logs: string[] = []
  let logChars = 0
  const admitted: Request[] = []
  const observed = new Set<number>()
  const unhandled = new Set<GuestPromise>()
  const jobs: GuestJob[] = []
  const microtasks: GuestJob[] = []
  const wakeups: GuestJob[] = []
  let steps = 0
  let ordinal = 0
  let callDepth = 0
  let executionDepth = 0
  let releasedSequence = -1
  let jobCompletions = 0
  let subscriptionOrder = 0
  let activeJob: GuestJob | undefined
  let rootReturnAdmissions: number | undefined
  let scopes: Map<string, Binding>[] = [new Map()]
  let mutableScopes: Set<string>[] = [new Set()]
  const missing = () => Arr.filter(admitted, request => !Arr.some(model.settlements, entry => entry.request.ordinal === request.ordinal))
  const prepare = function* (node: CallExpression): Generator<Result<Execution>, Result<Request>, unknown> {
    const tool = toolPath(node.callee)
    if (tool === undefined) return error("UnsupportedSyntax")
    const binding = paths.get(tool)
    if (binding === undefined) return error("UnknownTool")
    if (ordinal >= input.budgets.calls) return error("CallBudgetExceeded")
    const arg = node.arguments[0]
    const child = arg !== undefined ? (yield* visit(arg)) : undefined
    if (child?.tag === "guest") return error("InvalidCallInput")
    if (child !== undefined && child.tag !== "ok") return child
    const copied = arg !== undefined && child?.tag === "ok" && child.value.value !== undefined ? copyJson(child.value.value, { ...input.budgets, resultChars: input.budgets.callInputChars }) : undefined
    if (copied !== undefined && copied.tag === "error" && copied.reason === "ResultBudgetExceeded") return error("CallInputBudgetExceeded")
    if (copied !== undefined && copied.tag === "error" && copied.reason === "InvalidJson") return error("InvalidCallInput")
    if (copied !== undefined && copied.tag !== "ok") return copied
    const request: Request = copied?.tag === "ok" ? { ordinal, ...binding, origin: origin.value, input: copied.value } : { ordinal, ...binding, origin: origin.value }
    ordinal++
    admitted.push(request)
    return ok(request)
  }
  const lookup = (request: Request, visibleOnly = false): Result<typeof Settlement.Type | undefined> => {
    const entry = Option.getOrUndefined(Arr.findFirst(model.settlements, item => item.request.ordinal === request.ordinal))
    if (entry !== undefined && !sameRequest(entry.request, request, input.budgets)) return error("ReplayMismatch")
    if (visibleOnly && entry !== undefined && entry.sequence > releasedSequence) return ok(undefined)
    return ok(entry)
  }
  const releaseNext = (): Result<boolean> => {
    const entry = model.settlements[releasedSequence + 1]
    if (entry === undefined) return ok(false)
    if (entry.sequence !== releasedSequence + 1) return error("ReplayMismatch")
    const request = Option.getOrUndefined(Arr.findFirst(admitted, item => item.ordinal === entry.request.ordinal))
    if (request === undefined || !sameRequest(entry.request, request, input.budgets)) return error("ReplayMismatch")
    releasedSequence++
    const drained = drainJobs(entry.request.ordinal)
    if (drained.tag !== "ok") return drained
    return ok(true)
  }
  const observePromise = (initial: GuestPromise): Result<null> => {
    const stack: GuestPromise[] = [initial]
    const seen = new Set<GuestPromise>()
    while (stack.length !== 0) {
      if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
      const item = stack.pop()
      if (item === undefined || seen.has(item)) continue
      seen.add(item)
      if (item.kind === "Tool") observed.add(item.request.ordinal)
      else if (item.kind === "Adopted") stack.push(item.source)
      else if (item.kind === "Job") {
        item.observed = true
        for (const dependency of item.dependencies) stack.push(dependency)
        if (item.step.done === true && item.step.value.tag === "ok" && isPromise(item.step.value.value.value)) stack.push(item.step.value.value.value)
      }
    }
    return ok(null)
  }
  const registerPromise = (promise: GuestPromise): Result<null> => {
    if (activeJob !== undefined) activeJob.dependencies.add(promise)
    if (activeJob === undefined || activeJob.observed) return observePromise(promise)
    return ok(null)
  }
  const resolvePromise = (promise: GuestPromise): Result<Value> => {
    const registration = registerPromise(promise)
    if (registration.tag !== "ok") return registration
    let depth = 0
    const adopting: GuestJob[] = []
    const complete = (outcome: Result<Value>): Result<Value> => {
      if (outcome.tag === "ok" || outcome.tag === "guest") for (const job of adopting) {
        if (job.completionOrder === undefined) { job.completionOrder = jobCompletions++; job.completionSequence = releasedSequence }
      }
      return outcome
    }
    for (;;) {
      if (promise.kind === "Adopted") { if (++depth > input.budgets.callDepth) return error("CallDepthBudgetExceeded"); promise = promise.source; continue }
      if (promise.kind === "Job") {
        if (++depth > input.budgets.callDepth) return error("CallDepthBudgetExceeded")
        promise.handled = true
        unhandled.delete(promise)
        adopting.push(promise)
        if (promise.step.done !== true) {
          if (promise.step.value.tag === "pending" && promise.step.value.waiting !== undefined) return { tag: "pending", requests: missing(), waiting: promise.step.value.waiting, waitingJob: promise }
          return { tag: "pending", requests: missing(), waitingJob: promise }
        }
        const outcome = promise.step.value
        if (outcome.tag !== "ok") return complete(outcome)
        if (!isPromise(outcome.value.value)) return complete(ok(outcome.value.value))
        promise = outcome.value.value
        continue
      }
      break
    }
    if (promise.kind === "Resolved") return complete(ok(promise.value))
    if (promise.kind === "Rejected") { unhandled.delete(promise); return complete({ tag: "guest", kind: promise.errorKind, reason: promise.reason }) }
    const entry = lookup(promise.request, true)
    if (entry.tag !== "ok") return entry
    if (entry.value === undefined) return { tag: "pending", requests: missing(), waiting: promise.request.ordinal }
    releasedSequence = Math.max(releasedSequence, entry.value.sequence)
    if (entry.value.outcome._tag === "Void") return complete(ok(undefined))
    if (entry.value.outcome._tag === "Failed") return complete({ tag: "guest", kind: "Tool", reason: entry.value.outcome.reason })
    return complete(guestValue(entry.value))
  }
  const visit = function* (node: AnyNode): Generator<Result<Execution>, Result<Execution>, unknown> {
    if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
    if (executionDepth >= input.budgets.executionDepth) return error("ExecutionDepthBudgetExceeded")
    executionDepth++
    const value = yield* walk(node)
    executionDepth--
    if (value.tag === "pending") yield value
    return value
  }
  const walk = function* (node: AnyNode): Generator<Result<Execution>, Result<Execution>, unknown> {
    switch (node.type) {
      case "Program": case "BlockStatement": {
        if (node.type === "BlockStatement") { scopes.push(new Map()); mutableScopes.push(new Set()) }
        const scope = scopes[scopes.length - 1]!
        for (const statement of node.body) if (statement.type === "VariableDeclaration") for (const declaration of statement.declarations) {
          if (declaration.id.type !== "Identifier" || scope.has(declaration.id.name)) return error("DuplicateBinding")
          scope.set(declaration.id.name, uninitialized)
          if (statement.kind === "let") mutableScopes[mutableScopes.length - 1]!.add(declaration.id.name)
        }
        for (const item of node.body) { const child = yield* visit(item); if (child.tag !== "ok" || child.value.returned) { if (node.type === "BlockStatement") { scopes.pop(); mutableScopes.pop() } return child } }
        if (node.type === "BlockStatement") { scopes.pop(); mutableScopes.pop() }
        return result()
      }
      case "VariableDeclaration": for (const declaration of node.declarations) { const child = yield* visit(declaration); if (child.tag !== "ok") return child } return result()
      case "VariableDeclarator": { if (node.id.type !== "Identifier" || node.init == null) return error("UnsupportedSyntax"); const scope = scopes[scopes.length - 1]!; if (!scope.has(node.id.name) || scope.get(node.id.name) !== uninitialized) return error("DuplicateBinding"); const child = yield* visit(node.init); if (child.tag !== "ok") return child; scope.set(node.id.name, child.value.value); return result() }
      case "Identifier": for (let i = scopes.length - 1; i >= 0; i--) { const scope = scopes[i]!; if (scope.has(node.name)) { const value = scope.get(node.name); if (value === uninitialized) return guestError("UninitializedBinding"); return result(value) } } if (node.name === "undefined") return result(); return guestError("UnknownIdentifier")
      case "Literal": if (node.value === null || typeof node.value === "string" || typeof node.value === "boolean" || typeof node.value === "number" && Number.isFinite(node.value)) return result(node.value); return error("UnsupportedSyntax")
      case "ObjectExpression": { if (node.properties.length > input.budgets.jsonNodes) return error("JsonNodeBudgetExceeded"); const object: Record<string, Value> = Object.create(null); for (const property of node.properties) { if (property.type !== "Property") return error("UnsupportedSyntax"); const key = property.key.type === "Identifier" ? property.key.name : property.key.type === "Literal" ? String(property.key.value) : ""; if (forbidden.has(key) || Object.getOwnPropertyDescriptor(object, key) !== undefined) return error("InvalidObjectKey"); const child = yield* visit(property.value); if (child.tag !== "ok") return child; object[key] = child.value.value } return result(object) }
      case "ArrayExpression": { if (node.elements.length > input.budgets.jsonNodes) return error("JsonNodeBudgetExceeded"); const array: Value[] = []; for (const element of node.elements) { if (element === null) return error("UnsupportedSyntax"); const child = yield* visit(element); if (child.tag !== "ok") return child; array.push(child.value.value) } return result(array) }
      case "MemberExpression": {
        const child = yield* visit(node.object)
        if (child.tag !== "ok") return child
        const object = child.value.value
        let key: string
        if (!node.computed && node.property.type === "Identifier") key = node.property.name
        else {
          if (node.property.type === "PrivateIdentifier") return error("UnsupportedSyntax")
          const property = yield* visit(node.property)
          if (property.tag !== "ok") return property
          if (typeof property.value.value !== "string" && typeof property.value.value !== "number") return guestError("InvalidMember")
          key = String(property.value.value)
        }
        if (forbidden.has(key)) return error("InvalidMember")
        if (object === null || typeof object !== "object" || isClosure(object) || isPromise(object) || isDate(object)) return guestError("InvalidMember")
        const descriptor = Object.getOwnPropertyDescriptor(object, key)
        if (descriptor === undefined) return result()
        if (!("value" in descriptor)) return guestError("InvalidMember")
        return result(descriptor.value)
      }
      case "ArrowFunctionExpression": case "FunctionExpression": return result({ [closureBrand]: true, node, scopes: [...scopes], mutableScopes: [...mutableScopes] })
      case "CallExpression": {
        if (repeatCall(node.callee)) {
          if (node.callee.type !== "MemberExpression" || node.callee.object.type !== "Literal" || typeof node.callee.object.value !== "string") return error("UnsupportedSyntax")
          const argument = node.arguments[0]
          if (argument === undefined) return error("UnsupportedSyntax")
          const count = yield* visit(argument)
          if (count.tag !== "ok") return count
          const repetitions = count.value.value
          if (typeof repetitions !== "number" || !Number.isSafeInteger(repetitions) || repetitions < 0) return guestError("InvalidOperand")
          const text = node.callee.object.value
          if (text.length === 0) return result("")
          if (repetitions > Math.floor(input.budgets.resultChars / text.length)) return error("ResultBudgetExceeded")
          return result(Str.repeat(repetitions)(text))
        }
        if (consoleCall(node.callee)) {
          if (logs.length >= input.budgets.logs) return error("LogBudgetExceeded")
          const parts: string[] = []
          let used = 0
          for (const argument of node.arguments) {
            const item = yield* visit(argument)
            if (item.tag !== "ok") return item
            const value = item.value.value
            const separator = parts.length === 0 ? 0 : 1
            const remaining = input.budgets.logChars - logChars - used - separator
            if (remaining < 0) return error("LogBudgetExceeded")
            let text: string
            if (value === undefined) text = "undefined"
            else if (typeof value === "string") text = value
            else {
              const copied = copyJson(value, { ...input.budgets, resultChars: remaining })
              if (copied.tag === "error" && copied.reason === "ResultBudgetExceeded") return error("LogBudgetExceeded")
              if (copied.tag !== "ok") return copied
              text = logJson(copied.value)
            }
            if (text.length > remaining) return error("LogBudgetExceeded")
            used += separator + text.length
            parts.push(text)
          }
          logs.push(Arr.join(parts, " "))
          logChars += used
          return result()
        }
        const continuation = thenBatch(node)
        if (continuation !== undefined) {
          const upstream = yield* visit(continuation.source)
          if (upstream.tag !== "ok") return upstream
          if (!isPromise(upstream.value.value)) return guestError("InvalidCall")
          const handler = yield* visit(continuation.callback)
          if (handler.tag !== "ok") return handler
          if (!isClosure(handler.value.value) || handler.value.value.node.params.length > 1) return guestError("InvalidCall")
          const callback = handler.value.value
          const started = startJob(thenFrame(upstream.value.value, callback.node, continuation.kind), [...callback.scopes], [...callback.mutableScopes], callDepth)
          if (started.tag !== "ok") return started
          return result(started.value)
        }
        if (resolveCall(node.callee)) {
          const argument = node.arguments[0]
          if (argument === undefined) return result({ [promiseBrand]: true, kind: "Resolved", value: undefined })
          const evaluated = yield* visit(argument)
          if (evaluated.tag === "guest") return { ...evaluated, beforePromise: true }
          if (evaluated.tag !== "ok") return evaluated
          if (isPromise(evaluated.value.value)) return evaluated
          return result({ [promiseBrand]: true, kind: "Resolved", value: evaluated.value.value })
        }
        const ambient = ambientCall(node.callee)
        if (ambient === "clock") return result(input.ambient.at)
        if (ambient === "random") {
          randomState ^= randomState << 13
          randomState ^= randomState >>> 17
          randomState ^= randomState << 5
          return result((randomState >>> 0) / 4294967296)
        }
        if (node.callee.type === "Identifier") {
          const callable = yield* visit(node.callee)
          if (callable.tag !== "ok") return callable
          const closure = callable.value.value
          if (!isClosure(closure) || closure.node.params.length !== node.arguments.length) return guestError("InvalidCall")
          const values: Value[] = []
          for (const argument of node.arguments) {
            const evaluated = yield* visit(argument)
            if (evaluated.tag !== "ok") return evaluated
            values.push(evaluated.value.value)
          }
          const local = new Map<string, Binding>()
          for (let i = 0; i < closure.node.params.length; i++) {
            const param = closure.node.params[i]!
            if (param.type !== "Identifier" || local.has(param.name)) return guestError("InvalidCall")
            local.set(param.name, values[i])
          }
          if (callDepth >= input.budgets.callDepth) return error("CallDepthBudgetExceeded")
          if (closure.node.async) {
            const started = startJob(asyncFrame(closure.node.body), [...closure.scopes, local], [...closure.mutableScopes, new Set(local.keys())], callDepth + 1)
            if (started.tag !== "ok") return started
            return result(started.value)
          }
          callDepth++
          const previousScopes = scopes
          const previousMutable = mutableScopes
          scopes = [...closure.scopes, local]
          mutableScopes = [...closure.mutableScopes, new Set(local.keys())]
          const returned = yield* visit(closure.node.body)
          callDepth--
          scopes = previousScopes
          mutableScopes = previousMutable
          if (returned.tag !== "ok") return returned
          return result(returned.value.value)
        }
        const combine = batchCall(node.callee)
        if (combine !== undefined) {
          const argument = node.arguments[0]
          if (argument === undefined) return error("UnsupportedSyntax")
          const array = yield* visit(argument)
          if (array.tag !== "ok") return array
          if (!isValueArray(array.value.value)) return guestError("InvalidCall")
          const started = startJob(combineFrame(array.value.value, combine), [...scopes], [...mutableScopes], callDepth)
          if (started.tag !== "ok") return started
          return result(started.value)
        }
        const prepared = yield* prepare(node)
        if (prepared.tag !== "ok") return prepared
        return result({ [promiseBrand]: true, kind: "Tool", request: prepared.value })
      }
      case "NewExpression": {
        if (!dateConstructor(node.callee) || node.arguments.length > 1) return error("UnsupportedSyntax")
        const argument = node.arguments[0]
        if (argument === undefined) {
          if (Math.abs(input.ambient.at) > 8640000000000000) return guestError("InvalidOperand")
          return result({ [dateBrand]: true, time: Math.trunc(input.ambient.at) })
        }
        const value = yield* visit(argument)
        if (value.tag !== "ok") return value
        if (typeof value.value.value !== "number" || !Number.isFinite(value.value.value) || Math.abs(value.value.value) > 8640000000000000) return guestError("InvalidOperand")
        return result({ [dateBrand]: true, time: Math.trunc(value.value.value) })
      }
      case "AssignmentExpression": {
        if (node.left.type === "MemberExpression") {
          const target = yield* visit(node.left.object)
          if (target.tag !== "ok") return target
          const object = target.value.value
          let key: string
          if (!node.left.computed && node.left.property.type === "Identifier") key = node.left.property.name
          else {
            if (node.left.property.type === "PrivateIdentifier") return error("UnsupportedSyntax")
            const property = yield* visit(node.left.property)
            if (property.tag !== "ok") return property
            if (typeof property.value.value !== "string" && typeof property.value.value !== "number") return guestError("InvalidMember")
            key = String(property.value.value)
          }
          if (forbidden.has(key)) return error("InvalidMember")
          if (object === null || typeof object !== "object" || isClosure(object) || isPromise(object) || isDate(object)) return guestError("InvalidMember")
          if (Arr.isArray(object)) {
            if (!/^(0|[1-9][0-9]*)$/.test(key)) return error("InvalidMember")
            const index = Number(key)
            if (!Number.isSafeInteger(index) || index > object.length) return error("InvalidMember")
            if (index === object.length && object.length >= input.budgets.jsonNodes) return error("JsonNodeBudgetExceeded")
          } else if (Object.getOwnPropertyDescriptor(object, key) === undefined && Reflect.ownKeys(object).length >= input.budgets.jsonNodes) return error("JsonNodeBudgetExceeded")
          const child = yield* visit(node.right)
          if (child.tag !== "ok") return child
          Object.defineProperty(object, key, { value: child.value.value, writable: true, enumerable: true, configurable: true })
          return result(child.value.value)
        }
        if (node.left.type !== "Identifier") return error("UnsupportedSyntax")
        for (let i = scopes.length - 1; i >= 0; i--) {
          const scope = scopes[i]!
          if (!scope.has(node.left.name)) continue
          if (!mutableScopes[i]!.has(node.left.name)) return guestError("ImmutableBinding")
          if (scope.get(node.left.name) === uninitialized) return guestError("UninitializedBinding")
          const child = yield* visit(node.right)
          if (child.tag !== "ok") return child
          scope.set(node.left.name, child.value.value)
          return result(child.value.value)
        }
        return guestError("UnknownIdentifier")
      }
      case "UpdateExpression": {
        if (node.argument.type !== "Identifier") return error("UnsupportedSyntax")
        for (let i = scopes.length - 1; i >= 0; i--) {
          const scope = scopes[i]!
          if (!scope.has(node.argument.name)) continue
          if (!mutableScopes[i]!.has(node.argument.name)) return guestError("ImmutableBinding")
          const old = scope.get(node.argument.name)
          if (old === uninitialized) return guestError("UninitializedBinding")
          if (typeof old !== "number") return error("InvalidOperand")
          const value = node.operator === "++" ? old + 1 : old - 1
          if (!Number.isFinite(value)) return error("InvalidOperand")
          scope.set(node.argument.name, value)
          return result(node.prefix ? value : old)
        }
        return guestError("UnknownIdentifier")
      }
      case "TryStatement": {
        const body = yield* visit(node.block)
        if (body.tag !== "guest") return body
        if (node.handler == null || node.handler.param != null && node.handler.param.type !== "Identifier") return error("UnsupportedSyntax")
        const caught = new Map<string, Binding>()
        if (node.handler.param?.type === "Identifier") caught.set(node.handler.param.name, { message: body.reason })
        scopes.push(caught)
        mutableScopes.push(new Set())
        const recovered = yield* visit(node.handler.body)
        scopes.pop()
        mutableScopes.pop()
        return recovered
      }
      case "WhileStatement": {
        for (;;) {
          const condition = yield* visit(node.test)
          if (condition.tag !== "ok") return condition
          if (!truthy(condition.value.value)) return result()
          const body = yield* visit(node.body)
          if (body.tag !== "ok" || body.value.returned) return body
        }
      }
      case "AwaitExpression": {
        const value = yield* visit(node.argument)
        if (value.tag !== "ok") return value
        if (!isPromise(value.value.value)) { yield { tag: "pending", requests: missing(), microtask: true }; return value }
        let resolved = resolvePromise(value.value.value)
        while (resolved.tag === "pending") {
          yield resolved
          resolved = resolvePromise(value.value.value)
        }
        if (resolved.tag === "error") return resolved
        yield { tag: "pending", requests: missing(), microtask: true }
        if (resolved.tag !== "ok") return resolved
        return result(resolved.value)
      }
      case "ExpressionStatement": { const child = yield* visit(node.expression); if (child.tag !== "ok") return child; return result() }
      case "ReturnStatement": { if (node.argument == null) return result(undefined, true); const child = yield* visit(node.argument); if (child.tag !== "ok") return child; return result(child.value.value, true) }
      case "IfStatement": { const condition = yield* visit(node.test); if (condition.tag !== "ok") return condition; if (truthy(condition.value.value)) return yield* visit(node.consequent); if (node.alternate != null) return yield* visit(node.alternate); return result() }
      case "UnaryExpression": { const child = yield* visit(node.argument); if (node.operator === "typeof" && node.argument.type === "Identifier" && child.tag === "guest" && child.reason === "UnknownIdentifier") return result("undefined"); if (child.tag !== "ok") return child; if (node.operator === "typeof") return result(isClosure(child.value.value) ? "function" : typeof child.value.value); if (node.operator === "!") return result(!truthy(child.value.value)); if (node.operator === "+" && isDate(child.value.value)) return result(child.value.value.time); if (typeof child.value.value !== "number") return error("InvalidOperand"); if (node.operator === "+") return result(child.value.value); return result(-child.value.value) }
      case "BinaryExpression": { const a = yield* visit(node.left); if (a.tag !== "ok") return a; const b = yield* visit(node.right); if (b.tag !== "ok") return b; const left = a.value.value; const right = b.value.value; if (node.operator === "+" && typeof left === "string" && typeof right === "string") { if (left.length > input.budgets.resultChars - right.length) return error("ResultBudgetExceeded"); return result(left + right) } if (node.operator === "===") return result(left === right); if (node.operator === "!==") return result(left !== right); if (typeof left !== "number" || typeof right !== "number") return error("InvalidOperand"); if (node.operator === "+") { const value = left + right; if (!Number.isFinite(value)) return error("InvalidOperand"); return result(value) } if (node.operator === "-") { const value = left - right; if (!Number.isFinite(value)) return error("InvalidOperand"); return result(value) } if (node.operator === "*") { const value = left * right; if (!Number.isFinite(value)) return error("InvalidOperand"); return result(value) } if (node.operator === "/") { const value = left / right; if (!Number.isFinite(value)) return error("InvalidOperand"); return result(value) } if (node.operator === "<") return result(left < right); if (node.operator === ">") return result(left > right); if (node.operator === "<=") return result(left <= right); return result(left >= right) }
      default: return error("UnsupportedSyntax")
    }
  }
  function* adopt(value: Value): Generator<Result<Execution>, Result<Execution>, unknown> {
    if (!isPromise(value)) return result(value)
    let source = value
    let depth = 1
    while (source.kind === "Adopted") { if (++depth > input.budgets.callDepth) return error("CallDepthBudgetExceeded"); source = source.source }
    if (source.kind === "Job") depth += source.adoptionDepth
    if (depth > input.budgets.callDepth) return error("CallDepthBudgetExceeded")
    if (activeJob !== undefined) activeJob.adoptionDepth = depth
    yield { tag: "pending", requests: missing(), microtask: true }
    let settled = resolvePromise(value)
    while (settled.tag === "pending") { yield settled; settled = resolvePromise(value) }
    if (settled.tag === "error") return settled
    yield { tag: "pending", requests: missing(), microtask: true }
    if (settled.tag !== "ok") return settled
    return result(settled.value)
  }
  function* asyncFrame(body: AnyNode): Generator<Result<Execution>, Result<Execution>, unknown> {
    const evaluated = yield* visit(body)
    if (evaluated.tag !== "ok") return evaluated
    return yield* adopt(evaluated.value.value)
  }
  function* thenFrame(source: GuestPromise, callback: ArrowFunctionExpression | FunctionExpression, kind: "then" | "catch"): Generator<Result<Execution>, Result<Execution>, unknown> {
    let settled = resolvePromise(source)
    while (settled.tag === "pending") { yield settled; settled = resolvePromise(source) }
    if (settled.tag === "error") return settled
    yield { tag: "pending", requests: missing(), microtask: true }
    if (settled.tag === "guest" && kind === "then") return settled
    if (settled.tag === "ok" && kind === "catch") return result(settled.value)
    const argument = settled.tag === "guest" ? { message: settled.reason } : settled.value
    const param = callback.params[0]
    if (param != null && param.type !== "Identifier") return guestError("InvalidCall")
    if (callDepth >= input.budgets.callDepth) return error("CallDepthBudgetExceeded")
    callDepth++
    const parameters = new Map<string, Binding>()
    if (param?.type === "Identifier") parameters.set(param.name, argument)
    scopes.push(parameters)
    mutableScopes.push(new Set(parameters.keys()))
    if (callback.async) {
      const started = startJob(asyncFrame(callback.body), [...scopes], [...mutableScopes], callDepth)
      scopes.pop()
      mutableScopes.pop()
      callDepth--
      if (started.tag !== "ok") return started
      return yield* adopt(started.value)
    }
    const resumed = yield* visit(callback.body)
    scopes.pop()
    mutableScopes.pop()
    callDepth--
    if (resumed.tag === "guest" && resumed.beforePromise === true) return { ...resumed, beforePromise: false }
    if (resumed.tag !== "ok") return resumed
    return yield* adopt(resumed.value.value)
  }
  function* combineFrame(inputs: readonly Value[], combine: "all" | "race"): Generator<Result<Execution>, Result<Execution>, unknown> {
    function* finish(outcome: Result<Execution>): Generator<Result<Execution>, Result<Execution>, unknown> {
      if (inputs.length !== 0) yield { tag: "pending", requests: missing(), microtask: true }
      return outcome
    }
    const startOrder = jobCompletions
    const startSequence = releasedSequence
    for (;;) {
      const entries: Array<{ readonly kind: "Immediate"; readonly value: Value } | { readonly kind: "JobReady"; readonly value: Value; readonly order: number; readonly sequence: number } | { readonly kind: "JobRejected"; readonly reason: string; readonly errorKind: "Tool" | "Guest"; readonly order: number; readonly sequence: number } | { readonly kind: "Rejected"; readonly reason: string; readonly errorKind: "Tool" | "Guest" } | { readonly kind: "Pending"; readonly waiting?: number; readonly waitingJob?: GuestJob } | { readonly kind: "Tool"; readonly request: Request; readonly entry: typeof Settlement.Type | undefined }> = []
      for (const item of inputs) {
        if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
        if (!isPromise(item)) {
          entries.push({ kind: "Immediate", value: item })
          continue
        }
        let value: GuestPromise = item
        let depth = 0
        while (value.kind === "Adopted") { if (++depth > input.budgets.callDepth) return error("CallDepthBudgetExceeded"); value = value.source }
        if (value.kind === "Resolved") {
          entries.push({ kind: "Immediate", value: value.value })
          continue
        }
        if (value.kind === "Rejected") {
          unhandled.delete(value)
          entries.push({ kind: "Rejected", reason: value.reason, errorKind: value.errorKind })
          continue
        }
        if (value.kind === "Job") {
          const settled = resolvePromise(value)
          if (settled.tag === "error") return settled
          if (settled.tag === "pending") {
            if (settled.waitingJob !== undefined) entries.push({ kind: "Pending", waitingJob: settled.waitingJob })
            else if (settled.waiting !== undefined) entries.push({ kind: "Pending", waiting: settled.waiting })
            else entries.push({ kind: "Pending" })
            continue
          }
          if (settled.tag === "guest") { if (value.completionOrder !== undefined && value.completionOrder >= startOrder && value.completionSequence !== undefined) entries.push({ kind: "JobRejected", reason: settled.reason, errorKind: settled.kind, order: value.completionOrder, sequence: value.completionSequence }); else entries.push({ kind: "Rejected", reason: settled.reason, errorKind: settled.kind }); continue }
          if (value.completionOrder !== undefined && value.completionOrder >= startOrder && value.completionSequence !== undefined) entries.push({ kind: "JobReady", value: settled.value, order: value.completionOrder, sequence: value.completionSequence })
          else entries.push({ kind: "Immediate", value: settled.value })
          continue
        }
        const registration = registerPromise(value)
        if (registration.tag !== "ok") return registration
        const entry = lookup(value.request, true)
        if (entry.tag !== "ok") return entry
        entries.push({ kind: "Tool", request: value.request, entry: entry.value })
      }
      const waitingJobs: GuestJob[] = []
      for (const item of entries) if (item.kind === "Pending" && item.waitingJob !== undefined) waitingJobs.push(item.waitingJob)
      const waitingCalls: number[] = []
      for (const item of entries) {
        if (item.kind === "Pending" && item.waiting !== undefined) waitingCalls.push(item.waiting)
        if (item.kind === "Tool" && item.entry === undefined) waitingCalls.push(item.request.ordinal)
      }
      if (combine === "all") {
        for (const item of entries) {
          if (item.kind === "Rejected") return yield* finish({ tag: "guest", kind: item.errorKind, reason: item.reason })
          if (item.kind === "Tool" && item.entry?.outcome._tag === "Failed" && item.entry.sequence <= startSequence) return yield* finish({ tag: "guest", kind: "Tool", reason: item.entry.outcome.reason })
        }
        let firstJobFailure: Extract<typeof entries[number], { readonly kind: "JobRejected" }> | undefined
        for (const item of entries) if (item.kind === "JobRejected" && (firstJobFailure === undefined || item.sequence < firstJobFailure.sequence || item.sequence === firstJobFailure.sequence && item.order < firstJobFailure.order)) firstJobFailure = item
        let earliestFailure: typeof Settlement.Type | undefined
        for (const item of entries) {
          if (item.kind !== "Tool" || item.entry?.outcome._tag !== "Failed") continue
          if (earliestFailure === undefined || earliestFailure.sequence > releasedSequence && (item.entry.sequence <= releasedSequence || item.entry.sequence < earliestFailure.sequence)) earliestFailure = item.entry
        }
        if (firstJobFailure !== undefined && (earliestFailure === undefined || firstJobFailure.sequence < earliestFailure.sequence)) return yield* finish({ tag: "guest", kind: firstJobFailure.errorKind, reason: firstJobFailure.reason })
        if (earliestFailure?.outcome._tag === "Failed") { releasedSequence = Math.max(releasedSequence, earliestFailure.sequence); return yield* finish({ tag: "guest", kind: "Tool", reason: earliestFailure.outcome.reason }) }
        if (Arr.some(entries, item => item.kind === "Pending" || item.kind === "Tool" && item.entry === undefined)) { yield { tag: "pending", requests: missing(), waitingJobs, waitingCalls }; continue }
        const values: Value[] = []
        for (const item of entries) {
          if (item.kind === "Immediate" || item.kind === "JobReady") values.push(item.value)
          else if (item.kind === "Rejected") return yield* finish({ tag: "guest", kind: item.errorKind, reason: item.reason })
          else if (item.kind === "JobRejected") return yield* finish({ tag: "guest", kind: item.errorKind, reason: item.reason })
          else if (item.kind === "Pending") return error("InvalidModel")
          else if (item.entry?.outcome._tag === "Void") { releasedSequence = Math.max(releasedSequence, item.entry.sequence); values.push(undefined) }
          else if (item.entry?.outcome._tag === "Json") { releasedSequence = Math.max(releasedSequence, item.entry.sequence); const value = guestValue(item.entry); if (value.tag !== "ok") return value; values.push(value.value) }
          else return error("InvalidModel")
        }
        return yield* finish(result(values))
      }
      for (const item of entries) {
        if (item.kind === "Immediate") return yield* finish(result(item.value))
        if (item.kind === "Rejected") return yield* finish({ tag: "guest", kind: item.errorKind, reason: item.reason })
        if (item.kind === "Pending" || item.kind === "JobReady" || item.kind === "JobRejected") continue
        if (item.entry !== undefined && item.entry.sequence <= startSequence) {
          if (item.entry.outcome._tag === "Failed") return yield* finish({ tag: "guest", kind: "Tool", reason: item.entry.outcome.reason })
          if (item.entry.outcome._tag === "Json") { const value = guestValue(item.entry); if (value.tag !== "ok") return value; return yield* finish(result(value.value)) }
          return yield* finish(result())
        }
      }
      let earliest: typeof Settlement.Type | undefined
      for (const item of entries) if (item.kind === "Tool" && item.entry !== undefined && (earliest === undefined || item.entry.sequence < earliest.sequence)) earliest = item.entry
      let firstJob: Extract<typeof entries[number], { readonly kind: "JobReady" | "JobRejected" }> | undefined
      for (const item of entries) if ((item.kind === "JobReady" || item.kind === "JobRejected") && (firstJob === undefined || item.sequence < firstJob.sequence || item.sequence === firstJob.sequence && item.order < firstJob.order)) firstJob = item
      if (firstJob !== undefined && (earliest === undefined || firstJob.sequence < earliest.sequence)) { if (firstJob.kind === "JobRejected") return yield* finish({ tag: "guest", kind: firstJob.errorKind, reason: firstJob.reason }); return yield* finish(result(firstJob.value)) }
      if (earliest === undefined) { yield { tag: "pending", requests: missing(), waitingJobs, waitingCalls }; continue }
      releasedSequence = Math.max(releasedSequence, earliest.sequence)
      if (earliest.outcome._tag === "Failed") return yield* finish({ tag: "guest", kind: "Tool", reason: earliest.outcome.reason })
      if (earliest.outcome._tag === "Json") { const value = guestValue(earliest); if (value.tag !== "ok") return value; return yield* finish(result(value.value)) }
      return yield* finish(result())
    }
  }
  function startJob(jobFrame: Generator<Result<Execution>, Result<Execution>, unknown>, jobScopes: Map<string, Binding>[], jobMutableScopes: Set<string>[], depth: number, root = false): Result<GuestJob> {
    const job: GuestJob = { [promiseBrand]: true, kind: "Job", id: jobs.length, root, frame: jobFrame, dependencies: new Set(), subscriptions: new Map(), step: { done: false, value: { tag: "pending", requests: [] } }, scopes: jobScopes, mutableScopes: jobMutableScopes, callDepth: depth, executionDepth: 0, adoptionDepth: 0, completionOrder: undefined, completionSequence: undefined, observed: root, handled: root }
    jobs.push(job)
    const started = advanceJob(job)
    if (started.tag !== "ok") return started
    return ok(job)
  }
  function advanceJob(job: GuestJob): Result<null> {
    const previousScopes = scopes
    const previousMutable = mutableScopes
    const previousCallDepth = callDepth
    const previousExecutionDepth = executionDepth
    const previousJob = activeJob
    scopes = job.scopes
    mutableScopes = job.mutableScopes
    callDepth = job.callDepth
    executionDepth = job.executionDepth
    activeJob = job
    job.step = job.frame.next()
    if (job.root && job.step.done === true) rootReturnAdmissions = admitted.length
    job.scopes = scopes
    job.mutableScopes = mutableScopes
    job.callDepth = callDepth
    job.executionDepth = executionDepth
    if (job.step.done === true && (job.step.value.tag !== "ok" || !isPromise(job.step.value.value.value))) { job.completionOrder = jobCompletions++; job.completionSequence = releasedSequence }
    scopes = previousScopes
    mutableScopes = previousMutable
    callDepth = previousCallDepth
    executionDepth = previousExecutionDepth
    activeJob = previousJob
    if (job.step.value.tag === "error") return job.step.value
    const targets = new Set<string>()
    if (job.step.done !== true && job.step.value.tag === "pending" && job.step.value.microtask !== true && job.step.value.wake !== true) {
      if (job.step.value.waitingJob !== undefined) targets.add(`job:${job.step.value.waitingJob.id}`)
      else if (job.step.value.waiting !== undefined) targets.add(`call:${job.step.value.waiting}`)
      for (const dependency of job.step.value.waitingJobs ?? []) { if (++steps > input.budgets.steps) return error("StepBudgetExceeded"); targets.add(`job:${dependency.id}`) }
      for (const callOrdinal of job.step.value.waitingCalls ?? []) { if (++steps > input.budgets.steps) return error("StepBudgetExceeded"); targets.add(`call:${callOrdinal}`) }
    }
    for (const target of job.subscriptions.keys()) { if (++steps > input.budgets.steps) return error("StepBudgetExceeded"); if (!targets.has(target)) job.subscriptions.delete(target) }
    for (const target of targets) { if (++steps > input.budgets.steps) return error("StepBudgetExceeded"); if (!job.subscriptions.has(target)) job.subscriptions.set(target, subscriptionOrder++) }
    if (job.observed) { const observation = observePromise(job); if (observation.tag !== "ok") return observation }
    if (job.step.done !== true && job.step.value.tag === "pending" && job.step.value.microtask === true) microtasks.push(job)
    if (!job.handled && job.step.done === true && job.step.value.tag === "guest") unhandled.add(job)
    if (job.step.done === true) {
      const dependents = orderedJobs(`job:${job.id}`)
      if (dependents.tag !== "ok") return dependents
      for (const dependent of dependents.value) {
        if (dependent.step.done === true || dependent.step.value.tag !== "pending" || dependent.step.value.microtask === true || dependent.step.value.wake === true) continue
        dependent.step = { done: false, value: { ...dependent.step.value, wake: true } }
        wakeups.push(dependent)
      }
    }
    return ok(null)
  }
  function orderedJobs(target: string): Result<GuestJob[]> {
    const heap: Array<{ readonly job: GuestJob; readonly order: number }> = []
    for (const job of jobs) {
      if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
      const order = job.subscriptions.get(target)
      if (order === undefined) continue
      const item = { job, order }
      heap.push(item)
      let index = heap.length - 1
      while (index > 0) {
        if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
        const parent = Math.floor((index - 1) / 2)
        if (heap[parent]!.order <= order) break
        heap[index] = heap[parent]!
        index = parent
      }
      heap[index] = item
    }
    const ready: GuestJob[] = []
    while (heap.length !== 0) {
      if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
      const first = heap[0]!
      const last = heap.pop()!
      if (heap.length !== 0) {
        let index = 0
        for (;;) {
          if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
          const left = index * 2 + 1
          if (left >= heap.length) break
          const right = left + 1
          const child = right < heap.length && heap[right]!.order < heap[left]!.order ? right : left
          if (last.order <= heap[child]!.order) break
          heap[index] = heap[child]!
          index = child
        }
        heap[index] = last
      }
      ready.push(first.job)
    }
    return ok(ready)
  }
  function drainJobs(callOrdinal: number): Result<null> {
    const ready = orderedJobs(`call:${callOrdinal}`)
    if (ready.tag !== "ok") return ready
    for (const job of ready.value) {
      if (job.step.done === true || job.step.value.tag !== "pending" || job.step.value.microtask === true || job.step.value.wake === true || !job.subscriptions.has(`call:${callOrdinal}`)) continue
      const advanced = advanceJob(job)
      if (advanced.tag !== "ok") return advanced
    }
    return ok(null)
  }
  function drainWakeups(): Result<boolean> {
    let ran = false
    let index = 0
    while (index < wakeups.length) {
      if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
      const job = wakeups[index++]
      if (job === undefined || job.step.done === true || job.step.value.tag !== "pending" || job.step.value.wake !== true) return error("InvalidModel")
      ran = true
      const advanced = advanceJob(job)
      if (advanced.tag !== "ok") return advanced
    }
    wakeups.length = 0
    return ok(ran)
  }
  function drainMicrotasks(): Result<boolean> {
    const awakened = drainWakeups()
    if (awakened.tag !== "ok") return awakened
    let ran = awakened.value
    let index = 0
    while (index < microtasks.length) {
      if (++steps > input.budgets.steps) return error("StepBudgetExceeded")
      const job = microtasks[index++]
      if (job === undefined || job.step.done === true || job.step.value.tag !== "pending" || job.step.value.microtask !== true) return error("InvalidModel")
      ran = true
      const advanced = advanceJob(job)
      if (advanced.tag !== "ok") return advanced
      const subsequentWakeups = drainWakeups()
      if (subsequentWakeups.tag !== "ok") return subsequentWakeups
    }
    microtasks.length = 0
    return ok(ran)
  }
  function unhandledFailure(): Result<never> | undefined {
    for (const promise of unhandled) {
      if (promise.kind === "Rejected") return { tag: "guest", kind: promise.errorKind, reason: promise.reason, logs }
      if (promise.kind === "Job" && promise.step.done === true && promise.step.value.tag === "guest") return { ...promise.step.value, logs }
    }
    return undefined
  }
  function* executeRoot(): Generator<Result<Execution>, Result<Execution>, unknown> {
    const evaluated = yield* visit(program)
    if (evaluated.tag !== "ok" || !evaluated.value.returned || !isPromise(evaluated.value.value)) return evaluated
    const promise = evaluated.value.value
    let resolved = resolvePromise(promise)
    while (resolved.tag === "pending") { yield resolved; resolved = resolvePromise(promise) }
    if (resolved.tag === "error") return resolved
    yield { tag: "pending", requests: missing(), microtask: true }
    if (resolved.tag !== "ok") return resolved
    return result(resolved.value, true)
  }
  const started = startJob(executeRoot(), scopes, mutableScopes, 0, true)
  if (started.tag !== "ok") return started
  const root = started.value
  for (;;) {
    const drained = drainMicrotasks()
    if (drained.tag !== "ok") return drained
    if (root.step.done === true) break
    const failure = unhandledFailure()
    if (failure !== undefined) return failure
    const released = releaseNext()
    if (released.tag !== "ok") return released
    if (!released.value) break
  }
  const initialDrain = drainMicrotasks()
  if (initialDrain.tag !== "ok") return initialDrain
  if (rootReturnAdmissions !== undefined && admitted.length > rootReturnAdmissions) return error("UnsettledHostCalls")
  const evaluated = root.step.value
  if (evaluated.tag === "pending") { const failure = unhandledFailure(); if (failure !== undefined) return failure }
  if (evaluated.tag === "pending" && missing().length === 0) return error("UnsettledGuestJobs")
  if (evaluated.tag === "guest" && missing().length !== 0) {
    if (Arr.some(missing(), request => !observed.has(request.ordinal) && !Arr.some(model.pending, old => old.ordinal === request.ordinal))) return error("UnsettledHostCalls")
    return { tag: "pending", requests: missing(), logs }
  }
  if (evaluated.tag === "pending") return { ...evaluated, requests: missing(), logs }
  if (evaluated.tag === "guest") return { ...evaluated, logs }
  if (evaluated.tag !== "ok") return evaluated
  const returned = evaluated.value.value
  const admittedAtReturn = rootReturnAdmissions ?? admitted.length
  for (;;) {
    const drained = drainMicrotasks()
    if (drained.tag !== "ok") return drained
    if (admitted.length > admittedAtReturn) return error("UnsettledHostCalls")
    const released = releaseNext()
    if (released.tag !== "ok") return released
    if (!released.value) break
  }
  const unresolved = missing()
  const failure = unhandledFailure()
  if (failure !== undefined) return failure
  if (Arr.some(unresolved, request => !observed.has(request.ordinal) && !Arr.some(model.pending, old => old.ordinal === request.ordinal))) return error("UnsettledHostCalls")
  if (unresolved.length !== 0) return { tag: "pending", requests: unresolved, logs }
  if (ordinal !== model.settlements.length) return error("UnexpectedSettlement")
  if (!evaluated.value.returned || returned === undefined) return ok({ _tag: "Success", logs })
  if (isClosure(returned) || isPromise(returned) || isDate(returned)) return error("InvalidJson")
  const value = copyJson(returned, input.budgets)
  if (value.tag !== "ok") return value
  if (returned !== null && typeof returned === "object") {
    const original = provenance.get(returned)
    if (original?.outcome._tag === "Json" && equalJson(value.value, original.outcome.value)) return ok({ _tag: "Success", value: value.value, logs, returnedCallOrdinal: original.request.ordinal })
  }
  return ok({ _tag: "Success", value: value.value, logs })
}

export const initial: { (input: Input, catalog: typeof PackageCatalog.Type): Model; (catalog: typeof PackageCatalog.Type): (input: Input) => Model } = Fn.dual(2, (input: Input, catalog: typeof PackageCatalog.Type): Model => ({ input, catalog, settlements: [], pending: [], logs: [] }))
export const update: { (model: Model, message: Message): Model; (message: Message): (model: Model) => Model } = Fn.dual(2, (model: Model, message: Message): Model => {
  const fail = (reason: string): Model => ({ input: model.input, catalog: model.catalog, settlements: model.settlements, pending: [], logs: model.logs, outcome: { _tag: "Failure", reason } })
  if (model.outcome !== undefined && model.pending.length !== 0) return fail("InvalidModel")
  if (model.outcome !== undefined) return model
  if (!Schema.is(Budgets)(model.input.budgets)) return fail("InvalidBudget")
  if (message._tag === "Start" && (model.pending.length !== 0 || model.settlements.length !== 0)) return fail("UnexpectedStart")
  if (message._tag !== "Start") {
    if (message.sequence !== model.settlements.length || Arr.some(model.settlements, entry => entry.request.ordinal === message.ordinal)) return fail("UnexpectedSettlement")
    if (!Arr.some(model.pending, request => request.ordinal === message.ordinal)) return fail("UnexpectedSettlement")
    const expected = evaluate({ input: model.input, catalog: model.catalog, settlements: model.settlements, pending: model.pending, logs: model.logs })
    if (expected.tag === "error" && expected.reason === "ReplayMismatch") return fail("ReplayMismatch")
    if (expected.tag !== "pending" || expected.requests.length !== model.pending.length) return fail("UnexpectedSettlement")
    const seen = new Set<number>()
    for (const pending of model.pending) {
      if (seen.has(pending.ordinal)) return fail("UnexpectedSettlement")
      seen.add(pending.ordinal)
      const expectedRequest = Option.getOrUndefined(Arr.findFirst(expected.requests, request => request.ordinal === pending.ordinal))
      if (expectedRequest === undefined) return fail("UnexpectedSettlement")
      if (!sameRequest(pending, expectedRequest, model.input.budgets)) return fail("UnexpectedSettlement")
    }
    if (message._tag === "Failed" && message.reason.length > model.input.budgets.resultChars) return fail("ResultBudgetExceeded")
    const value = message._tag === "Settled" && message.value !== undefined ? decodeHostJson(message.value, model.input.budgets) : undefined
    if (value !== undefined && value.tag !== "ok") return fail(value.tag === "error" ? value.reason : "InvalidJson")
    const outcome: typeof SettlementOutcome.Type = message._tag === "Failed" ? { _tag: "Failed", reason: message.reason } : value === undefined ? { _tag: "Void" } : value.tag === "ok" ? { _tag: "Json", value: value.value } : { _tag: "Void" }
    const request = Option.getOrUndefined(Arr.findFirst(model.pending, item => item.ordinal === message.ordinal))
    if (request === undefined) return fail("UnexpectedSettlement")
    const settlement: typeof Settlement.Type = { request, sequence: message.sequence, outcome }
    const next: Model = { input: model.input, catalog: model.catalog, settlements: [...model.settlements, settlement], pending: Arr.filter(model.pending, item => item.ordinal !== message.ordinal), logs: model.logs }
    const evaluated = evaluate(next)
    if (evaluated.tag === "error") return { ...next, outcome: { _tag: "Failure", reason: evaluated.reason } }
    if (evaluated.tag === "guest") return { ...next, logs: evaluated.logs ?? [], outcome: { _tag: evaluated.kind === "Tool" ? "ToolFailure" : "GuestFailure", reason: evaluated.reason } }
    if (evaluated.tag === "pending") return { ...next, pending: evaluated.requests, logs: evaluated.logs ?? [] }
    return { ...next, logs: evaluated.value._tag === "Success" ? evaluated.value.logs : [], outcome: evaluated.value }
  }
  const evaluated = evaluate(model)
  if (evaluated.tag === "error") return fail(evaluated.reason)
  if (evaluated.tag === "guest") return { ...model, logs: evaluated.logs ?? [], outcome: { _tag: evaluated.kind === "Tool" ? "ToolFailure" : "GuestFailure", reason: evaluated.reason } }
  if (evaluated.tag === "pending") return { ...model, pending: evaluated.requests, logs: evaluated.logs ?? [] }
  return { ...model, logs: evaluated.value._tag === "Success" ? evaluated.value.logs : [], outcome: evaluated.value }
})

export type Tools<R> = Readonly<Record<string, Readonly<Record<string, (request: Request) => Effect.Effect<string | undefined, string, R>>>>>
export const run = <R>(tools: Tools<R>) => (input: Input): Effect.Effect<Outcome, never, Exclude<R, Scope.Scope>> => {
  if (!Schema.is(Budgets)(input.budgets)) return Effect.succeed({ _tag: "Failure", reason: "InvalidBudget" })
  return Effect.gen(function* () {
    const clock = yield* Clock.Clock
    const start = yield* Clock.currentTimeMillis
    const expired = () => Clock.currentTimeMillis.pipe(Effect.map(now => now - start >= input.budgets.deadlineMs))
    let model = update(initial(input, Rec.map(tools, Rec.keys)), { _tag: "Start" })
    const completions = yield* Queue.unbounded<{ readonly _tag: "Deadline" } | { readonly _tag: "Fatal"; readonly cause: Cause.Cause<never> } | { readonly _tag: "Tool"; readonly request: Request; readonly outcome: EffectResult.Result<string | undefined, string> }>()
    const launched = new Set<number>()
    while (model.pending.length !== 0) {
      if (yield* expired()) return { _tag: "Failure" as const, reason: "DeadlineExceeded" }
      for (const request of model.pending) {
        if (launched.has(request.ordinal)) continue
        launched.add(request.ordinal)
        const task = Effect.suspend(() => {
          if (clock.currentTimeMillisUnsafe() - start >= input.budgets.deadlineMs) return Queue.offer(completions, { _tag: "Deadline" })
          return Effect.scoped(tools[request.package]![request.method]!(request)).pipe(
            Effect.result,
            Effect.flatMap(outcome => Queue.offer(completions, { _tag: "Tool", request, outcome })),
          )
        }).pipe(Effect.catchCause(cause => Queue.offer(completions, { _tag: "Fatal", cause })))
        yield* Effect.forkChild(task)
      }
      const completion = yield* Queue.take(completions)
      if (completion._tag === "Fatal") return yield* Effect.failCause(completion.cause)
      if (completion._tag === "Deadline" || (yield* expired())) return { _tag: "Failure" as const, reason: "DeadlineExceeded" }
      const request = completion.request
      const toolResult = completion.outcome
      if (toolResult._tag === "Failure") model = update(model, { _tag: "Failed", ordinal: request.ordinal, sequence: model.settlements.length, reason: typeof toolResult.failure === "string" ? toolResult.failure : "UnknownToolFailure" })
      else if (toolResult.success === undefined) model = update(model, { _tag: "Settled", ordinal: request.ordinal, sequence: model.settlements.length })
      else model = update(model, { _tag: "Settled", ordinal: request.ordinal, sequence: model.settlements.length, value: toolResult.success })
    }
    if (yield* expired()) return { _tag: "Failure" as const, reason: "DeadlineExceeded" }
    return model.outcome ?? { _tag: "Failure" as const, reason: "InvalidModel" }
  }).pipe(Effect.scoped, Effect.timeoutOrElse({ duration: input.budgets.deadlineMs, orElse: () => Effect.succeed({ _tag: "Failure" as const, reason: "DeadlineExceeded" }) }))
}
