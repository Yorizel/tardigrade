import { expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"
import { ResourceGrid } from "../diagrams/verification/FactoryPathsDiagram"

test("exposes the resource state as a named image", () => {
  const markup = renderToStaticMarkup(<svg><ResourceGrid water={12} clips={500} /></svg>)
  expect(markup).toContain('role="img"')
  expect(markup).toContain('aria-label="10 of 10 water units, 2 units spilled, 500 of 1000 paperclips"')
})
