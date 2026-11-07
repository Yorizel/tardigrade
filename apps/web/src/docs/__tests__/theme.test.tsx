import { expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"
import { ThemeToggle } from "../../site/ThemeToggle"

test("renders a stable theme control during server rendering", () => {
  const markup = renderToStaticMarkup(<ThemeToggle />)
  expect(markup).toContain('aria-label="Use dark mode"')
  expect(markup).toContain('aria-pressed="false"')
})
