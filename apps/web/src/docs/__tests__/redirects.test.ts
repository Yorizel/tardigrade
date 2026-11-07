import { describe, expect, test } from "bun:test"
import { docsRedirect } from "../redirects"

describe("documentation redirects", () => {
  test("preserves Markdown suffix, query and fragment", () => {
    expect(docsRedirect(new URL("https://example.test/docs/why.mdx?view=1#detail"))).toBe("/notes/why-tardigrade.mdx?view=1#detail")
  })

  test("ignores paths without a redirect", () => {
    expect(docsRedirect(new URL("https://example.test/docs/architecture"))).toBeUndefined()
  })
})
