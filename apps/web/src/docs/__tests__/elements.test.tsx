import { describe, expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"
import { Link, Math } from "../components/elements"

describe("documentation elements", () => {
  test("keeps accessible link content and external navigation attributes", () => {
    const markup = renderToStaticMarkup(<Link href="https://example.test">Example</Link>)
    expect(markup).toContain(">Example</a>")
    expect(markup).toContain('target="_blank"')
    expect(markup).toContain('rel="noopener noreferrer"')
  })

  test("renders math content", () => {
    expect(renderToStaticMarkup(<Math expression="x^2" />)).toContain("katex")
  })
})
