import { expect, test } from "bun:test"
import { Array as Arr } from "effect"
import { check } from "../docs-lint"

test("prose rules reject violations and skip fenced examples", () => {
  const text = ["first line", "continued line", "", "Previously this worked.", "", "Use an em — dash.", "", "```", "Previously —", "```"]
  const problems = check("fixture.md")(Arr.join(text, "\n"))
  expect(Arr.map(problems, problem => problem.rule)).toEqual(["wrapped", "history", "dash"])
})
