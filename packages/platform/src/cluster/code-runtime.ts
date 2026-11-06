import { CodeRuntime, CodeRuntimeRejected, GuestCall } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { CodeCall, CodeOutcome, PackageCatalog } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Array as Arr, Clock, Effect, Option, Order, Record, Result, Schema } from "effect"
import { getQuickJS, type QuickJSDeferredPromise, type QuickJSHandle } from "quickjs-emscripten"

const GuestJson = Schema.fromJsonString(GuestCall)
const OutcomeJson = Schema.fromJsonString(CodeOutcome)
const JsonText = Schema.fromJsonString(Schema.Json)

export const CodeRuntimeLive = CodeRuntime.toLayer(Effect.succeed({
  evaluate: Effect.fn("QuickJS.evaluate")(function* ({ request, calls }) {
    return yield* Effect.scoped(Effect.gen(function* () {
      if (request.code.length > request.limits.maxCodeChars) return yield* CodeRuntimeRejected.make({ reason: "CodeBudgetExceeded" })
      const catalog = request.catalog
      const clock = yield* Clock.clockWith(Effect.succeed)
      const module = yield* Effect.tryPromise({ try: () => getQuickJS(), catch: cause => CodeRuntimeRejected.make({ reason: "VMInitializationFailed", cause }) })
      const runtime = yield* Effect.acquireRelease(Effect.try({ try: () => module.newRuntime(), catch: cause => CodeRuntimeRejected.make({ reason: "VMRuntimeAllocationFailed", cause }) }), value => Effect.sync(() => value.dispose()))
      runtime.setMemoryLimit(request.limits.maxMemoryBytes)
      runtime.setMaxStackSize(request.limits.maxStackBytes)
      const startedAt = clock.currentTimeMillisUnsafe()
      const interrupted: boolean[] = []
      runtime.setInterruptHandler(() => {
        if (clock.currentTimeMillisUnsafe() - startedAt < request.limits.maxSegmentMs) return false
        interrupted.push(true)
        return true
      })
      const vm = yield* Effect.acquireRelease(Effect.try({ try: () => runtime.newContext(), catch: cause => CodeRuntimeRejected.make({ reason: "VMContextAllocationFailed", cause }) }), value => Effect.sync(() => value.dispose()))
      const promises = yield* Effect.acquireRelease(Effect.sync(() => new Map<number, QuickJSDeferredPromise>()), value => Effect.sync(() => value.forEach(promise => promise.dispose())))
      const resolvedObjects = new Map<number, QuickJSHandle>()
      const returnedOrdinals: number[] = []
      const observed: Array<typeof GuestCall.Type> = []
      const logs: string[] = []
      const failures: string[] = []
      const jobs: number[] = []
      const reject = (reason: string) => {
        failures.push(reason)
        const error = vm.newError(reason)
        return { error }
      }
      const call = vm.newFunction("request", handle => {
        const decoded = Schema.decodeResult(GuestJson)(vm.getString(handle))
        if (Result.isFailure(decoded)) return reject("InvalidCallInput")
        const next = decoded.success
        if (next.ordinal !== observed.length) return reject("CallOrdinalMismatch")
        if (observed.length >= request.limits.maxCalls) return reject("CallBudgetExceeded")
        if (next.input !== undefined) {
          const encoded = Schema.encodeResult(JsonText)(next.input)
          if (Result.isFailure(encoded)) return reject("InvalidCallInput")
          if (encoded.success.length > request.limits.maxCallInputChars) return reject("CallInputBudgetExceeded")
        }
        if (!Record.has(catalog, next.package) || catalog[next.package]?.includes(next.method) !== true) return reject("UnknownMethod")
        const recorded = Option.getOrUndefined(Arr.findFirst(calls, value => value.request.ordinal === next.ordinal))
        if (recorded !== undefined && !Schema.toEquivalence(GuestCall)(next, recorded.request)) return reject("ReplayIdentityMismatch")
        observed.push(next)
        const deferred = vm.newPromise()
        promises.set(next.ordinal, deferred)
        return deferred.handle
      })
      vm.setProp(vm.global, "__request", call)
      call.dispose()
      const log = vm.newFunction("log", handle => {
        const value = vm.getString(handle)
        if (logs.length >= request.limits.maxLogs || Arr.join(logs, "").length + value.length > request.limits.maxLogChars) return reject("LogBudgetExceeded")
        logs.push(value)
        return vm.undefined
      })
      vm.setProp(vm.global, "__log", log)
      log.dispose()
      const returned = vm.newFunction("returned", handle => {
        if (vm.typeof(handle) !== "object" || vm.eq(handle, vm.null)) return vm.undefined
        const original = Option.getOrUndefined(Arr.findFirst([...resolvedObjects], ([, value]) => vm.eq(handle, value)))
        if (original !== undefined) returnedOrdinals.push(original[0])
        return vm.undefined
      })
      vm.setProp(vm.global, "__returned", returned)
      returned.dispose()
      const evaluate = Effect.fn("QuickJS.eval")(function* (code: string) {
        const result = vm.evalCode(code)
        if (result.error !== undefined) {
          const reason = String(vm.dump(result.error))
          result.error.dispose()
          return yield* CodeRuntimeRejected.make({ reason: `GuestEvaluationFailed: ${reason}` })
        }
        return yield* Effect.acquireRelease(Effect.succeed(result.value), value => Effect.sync(() => value.dispose()))
      })
      const drain = Effect.fn("QuickJS.drain")(function* () {
        while (runtime.hasPendingJob()) {
          if (jobs.length >= request.limits.maxJobs) return yield* CodeRuntimeRejected.make({ reason: "JobBudgetExceeded" })
          jobs.push(1)
          const result = runtime.executePendingJobs(1)
          if (result.error !== undefined) {
            const reason = String(vm.dump(result.error))
            result.error.dispose()
            return yield* CodeRuntimeRejected.make({ reason: `GuestJobFailed: ${reason}` })
          }
        }
        if (interrupted.length > 0) return yield* CodeRuntimeRejected.make({ reason: "SegmentBudgetExceeded" })
        if (failures.length > 0) return yield* CodeRuntimeRejected.make({ reason: Arr.join(failures, ", ") })
      })
      const atJson = yield* Schema.encodeEffect(JsonText)(request.ambient.at).pipe(Effect.orDie)
      const seedJson = yield* Schema.encodeEffect(JsonText)(request.ambient.seed).pipe(Effect.orDie)
      const codeJson = yield* Schema.encodeEffect(JsonText)(request.code).pipe(Effect.orDie)
      const catalogJson = yield* Schema.encodeEffect(Schema.fromJsonString(PackageCatalog))(catalog).pipe(Effect.orDie)
      const program = `(() => {
        const request = globalThis.__request;
        const log = globalThis.__log;
        const returned = globalThis.__returned;
        delete globalThis.__request;
        delete globalThis.__log;
        delete globalThis.__returned;
        const stringify = JSON.stringify;
        const create = Object.create;
        const prototypeOf = Object.getPrototypeOf;
        const setPrototypeOf = Object.setPrototypeOf;
        const descriptorOf = Object.getOwnPropertyDescriptor;
        const ownKeys = Reflect.ownKeys;
        const hasOwn = Object.hasOwn;
        const isArray = Array.isArray;
        const isFinite = Number.isFinite;
        const isInteger = Number.isInteger;
        const number = Number;
        const string = String;
        const objectPrototype = Object.prototype;
        const arrayPrototype = Array.prototype;
        const NativeSet = Set;
        const every = Function.prototype.call.bind(Array.prototype.every);
        const setHas = Function.prototype.call.bind(Set.prototype.has);
        const setAdd = Function.prototype.call.bind(Set.prototype.add);
        const setDelete = Function.prototype.call.bind(Set.prototype.delete);
        const jsonInput = (value, ancestors) => {
          if (value === null || typeof value === 'string' || typeof value === 'boolean') return { valid: true, value };
          if (typeof value === 'number') return { valid: isFinite(value), value };
          if (typeof value !== 'object' || setHas(ancestors, value)) return { valid: false };
          const array = isArray(value);
          const prototype = prototypeOf(value);
          if (array && prototype !== arrayPrototype) return { valid: false };
          if (!array && prototype !== objectPrototype && prototype !== null) return { valid: false };
          const keys = ownKeys(value);
          const copy = create(null);
          const items = [];
          setPrototypeOf(items, null);
          const length = descriptorOf(value, 'length');
          if (array && (length === undefined || keys.length !== length.value + 1)) return { valid: false };
          setAdd(ancestors, value);
          const valid = every(keys, key => {
            if (array && key === 'length') return true;
            if (typeof key !== 'string') return false;
            if (array && (!isInteger(number(key)) || number(key) < 0 || number(key) >= length.value || string(number(key)) !== key)) return false;
            const descriptor = descriptorOf(value, key);
            if (descriptor === undefined || !hasOwn(descriptor, 'value') || descriptor.enumerable !== true) return false;
            const child = jsonInput(descriptor.value, ancestors);
            if (!child.valid) return false;
            if (array) items[key] = child.value;
            else copy[key] = child.value;
            return true;
          });
          setDelete(ancestors, value);
          if (array) return { valid, value: items };
          return { valid, value: copy };
        };
        const OriginalDate = Date;
        const at = ${atJson};
        function PinnedDate(...args) {
          if (!new.target) return new OriginalDate(at).toString();
          if (args.length === 0) return new OriginalDate(at);
          return Reflect.construct(OriginalDate, args);
        }
        PinnedDate.prototype = OriginalDate.prototype;
        Object.defineProperty(PinnedDate.prototype, 'constructor', { value: PinnedDate });
        PinnedDate.now = () => at;
        PinnedDate.parse = OriginalDate.parse;
        PinnedDate.UTC = OriginalDate.UTC;
        globalThis.Date = PinnedDate;
        const randomState = [2166136261];
        for (const character of ${seedJson}) randomState[0] = Math.imul(randomState[0] ^ character.charCodeAt(0), 16777619) >>> 0;
        Math.random = () => { randomState[0] = (Math.imul(randomState[0], 1664525) + 1013904223) >>> 0; return randomState[0] / 4294967296; };
        const ordinal = [0];
        const catalog = ${catalogJson};
        for (const [name, methods] of Object.entries(catalog)) {
          const namespace = Object.create(null);
          for (const method of methods) namespace[method] = input => {
            const call = create(null);
            if (input !== undefined) {
              const decoded = jsonInput(input, new NativeSet());
              if (!decoded.valid) return request('');
              call.input = decoded.value;
            }
            call.ordinal = ordinal[0];
            call.package = name;
            call.method = method;
            ordinal[0] += 1;
            return request(stringify(call));
          };
          Object.defineProperty(globalThis, name, { value: Object.freeze(namespace) });
        }
        globalThis.console = Object.freeze({ log: (...values) => log(values.map(value => { if (typeof value === 'string') return value; if (value === undefined) return 'undefined'; return JSON.stringify(value); }).join(' ')) });
        const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
        const NativePromise = Promise;
        const then = Function.prototype.call.bind(Promise.prototype.then);
        return new NativePromise(resolve => then(new AsyncFunction(${codeJson})(), value => {
          const outcome = create(null);
          outcome._tag = 'Returned';
          if (value !== undefined) {
            const decoded = jsonInput(value, new NativeSet());
            if (decoded.valid) {
              outcome.value = decoded.value;
              returned(value);
            }
            else {
              outcome._tag = 'Failed';
              outcome.reason = 'InvalidGuestResult';
            }
          }
          resolve(stringify(outcome));
        }, error => {
          const outcome = create(null);
          outcome._tag = 'Failed';
          outcome.reason = string(error);
          resolve(stringify(outcome));
        }));
      })()`
      const parseJson = yield* evaluate("JSON.parse")
      const result = yield* evaluate(program)
      yield* drain()
      const settled = Arr.sortWith(Arr.filter(calls, value => value.settlementSequence !== null), (value: typeof CodeCall.Type) => value.settlementSequence ?? 0, Order.Number)
      yield* Effect.forEach(settled, Effect.fn("QuickJS.releaseSettlement")(function* (recorded) {
        const promise = promises.get(recorded.request.ordinal)
        if (promise === undefined || recorded.outcome === null) return yield* CodeRuntimeRejected.make({ reason: "ReplaySettlementNotObserved" })
        const outcome = recorded.outcome
        if (outcome._tag === "Failed") {
          const error = vm.newError(outcome.reason)
          promise.reject(error)
          error.dispose()
        } else if (outcome.value === undefined) promise.resolve()
        else {
          const encoded = yield* Schema.encodeEffect(JsonText)(outcome.value).pipe(Effect.mapError(cause => CodeRuntimeRejected.make({ reason: "ReplayValueEncodeFailed", cause })))
          const json = yield* Effect.acquireRelease(Effect.sync(() => vm.newString(encoded)), value => Effect.sync(() => value.dispose()))
          const parsed = vm.callFunction(parseJson, vm.undefined, json)
          if (parsed.error !== undefined) {
            parsed.error.dispose()
            return yield* CodeRuntimeRejected.make({ reason: "ReplayValueDecodeFailed" })
          }
          const value = yield* Effect.acquireRelease(Effect.succeed(parsed.value), handle => Effect.sync(() => handle.dispose()))
          if (vm.typeof(value) === "object" && !vm.eq(value, vm.null)) resolvedObjects.set(recorded.request.ordinal, value)
          promise.resolve(value)
        }
        yield* drain()
      }), { discard: true })
      if (Arr.some(calls, value => !Arr.some(observed, next => next.ordinal === value.request.ordinal))) return yield* CodeRuntimeRejected.make({ reason: "RecordedCallOmitted" })
      const state = vm.getPromiseState(result)
      if (state.type === "rejected") {
        const reason = String(vm.dump(state.error))
        state.error.dispose()
        return yield* CodeRuntimeRejected.make({ reason: `InvalidGuestResult: ${reason}` })
      }
      const requested = Arr.filter(observed, value => !Arr.some(calls, recorded => recorded.request.ordinal === value.ordinal))
      const unfinished = Arr.some(observed, value => !Arr.some(calls, recorded => recorded.request.ordinal === value.ordinal && recorded.settlementSequence !== null))
      if (state.type === "pending") {
        if (!unfinished) return yield* CodeRuntimeRejected.make({ reason: "UnresolvedGuestPromise" })
        return { _tag: "Suspended" as const, requested, observedCalls: observed.length, logs }
      }
      const text = vm.getString(state.value)
      state.value.dispose()
      const outcome = yield* Schema.decodeEffect(OutcomeJson)(text).pipe(Effect.mapError(() => CodeRuntimeRejected.make({ reason: "InvalidGuestResult" })))
      if (text.length > request.limits.maxResultChars) return yield* CodeRuntimeRejected.make({ reason: "ResultBudgetExceeded" })
      if (unfinished) return { _tag: "Suspended" as const, requested, observedCalls: observed.length, logs }
      const returnedCallOrdinal = returnedOrdinals[0]
      if (returnedCallOrdinal !== undefined) return { _tag: "Completed" as const, outcome, observedCalls: observed.length, logs, returnedCallOrdinal }
      return { _tag: "Completed" as const, outcome, observedCalls: observed.length, logs }
    }))
  }),
}))
