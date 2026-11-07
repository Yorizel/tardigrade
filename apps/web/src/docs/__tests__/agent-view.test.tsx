import { expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"
import { AgentViewDiagram } from "../../notes/diagrams/AgentViewDiagram"

test("describes the workspace and its possible storage backends", () => {
  const markup = renderToStaticMarkup(<AgentViewDiagram />)
  expect(markup).toContain("The agent writes buy milk to /notes/todo.md")
  expect(markup).toContain("carrier pigeon")
  expect(markup).toContain("filesystem")
  expect(markup).toContain("sqlite")
})
