import { expect, test } from "bun:test"
import { CodeMode } from "@opencode/codemode"
import { Effect } from "effect"

test("ambient time and random are private to each execution", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = CodeMode.make({ ambient: { at: 1234, seed: "seed" } })
  const code = "return [Date.now(), new Date().getTime(), new Date.prototype.constructor().getTime(), new Date(0).getTime(), Math.random(), Math.random()]"
  const results = yield* Effect.all([runtime.execute(code), runtime.execute(code)], { concurrency: "unbounded" })
  expect(results[0]).toEqual(results[1])
  if (results[0]?.ok && Array.isArray(results[0].value)) expect(results[0].value).toEqual([1234, 1234, 1234, 0, 0.5232248369138688, 0.0577320302836597])
  else expect(results[0]?.ok).toBe(true)
})))

test("ambient dates use UTC across host time zones", () => {
  const script = `
    import { CodeMode } from "@opencode/codemode";
    import { Effect } from "effect";
    const code = ${JSON.stringify(`
      const d = new Date(2024, 0, 2, 3, 4, 5);
      const before = [d.getFullYear(), d.getMonth(), d.getDate(), d.getDay(), d.getHours(), d.getMinutes(), d.getSeconds(), d.getMilliseconds(), d.getTimezoneOffset(), d.toDateString(), d.toTimeString()];
      d.setHours(12); d.setMinutes(13); d.setSeconds(14); d.setMilliseconds(15); d.setDate(3); d.setMonth(1); d.setFullYear(2025);
      return [before, d.toISOString(), Date.parse('2024-01-02T03:04:05'), new Date('2024-01-02T03:04:05').toISOString(), Date.parse('2024-01-02T03:04:05-03:00'), typeof d.getYear, typeof d.setYear, Math.random(), Date.parse('+010000-01-01T00:00:00.000Z'), new Date(253402300800000).toISOString(), new Date(new Date(253402300800000).toISOString()).getTime(), Date.parse('-000001-01-01T00:00:00.000Z')];
    `)};
    console.log(JSON.stringify(await Effect.runPromise(CodeMode.execute({ code, ambient: { at: 1234, seed: "seed" } }))));
  `
  const run = (tz: string) => Bun.spawnSync([process.execPath, "-e", script], { env: { ...process.env, TZ: tz }, stdout: "pipe", stderr: "pipe" })
  const utc = run("UTC")
  const saoPaulo = run("America/Sao_Paulo")
  expect(utc.exitCode).toBe(0)
  expect(saoPaulo.exitCode).toBe(0)
  expect(utc.stdout.toString()).toBe(saoPaulo.stdout.toString())
  const result = JSON.parse(utc.stdout.toString())
  expect(result.ok).toBe(true)
  expect(result.value).toEqual([[2024, 0, 2, 2, 3, 4, 5, 0, 0, "Tue Jan 02 2024", "03:04:05 GMT+0000 (Coordinated Universal Time)"], "2025-02-03T12:13:14.015Z", 1704164645000, "2024-01-02T03:04:05.000Z", 1704175445000, "undefined", "undefined", 0.5232248369138688, 253402300800000, "+010000-01-01T00:00:00.000Z", 253402300800000, -62198755200000])
})

for (const [seed, expected] of [["😀", [0.5629011185374111, 0.2204014570452273]], ["a😀é", [0.481099956901744, 0.14182984828948975]]] as const) {
  test(`ambient random preserves legacy seed iteration for ${seed}`, () => Effect.runPromise(Effect.gen(function* () {
    const result = yield* CodeMode.execute({ code: "return [Math.random(), Math.random()]", ambient: { at: 0, seed } })
    expect(result.ok).toBe(true)
    if (result.ok) expect(result.value).toEqual(expected)
  })))
}
