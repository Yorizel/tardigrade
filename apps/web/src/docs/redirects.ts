import { String as Str } from "effect"

const redirects: Readonly<Record<string, string>> = {
  "/docs/why": "/notes/why-tardigrade",
  "/docs/why-tardigrade": "/notes/why-tardigrade",
  "/docs/forms-and-shadows": "/notes/forms-and-shadows",
  "/docs/notes": "/notes",
  "/docs/notes/why-tardigrade": "/notes/why-tardigrade",
  "/docs/notes/forms-and-shadows": "/notes/forms-and-shadows",
  "/notes/telling-forms-from-their-shadows": "/notes/forms-and-shadows",
  "/notes/telling-forms-from-the-shadows": "/notes/forms-and-shadows",
}

export function docsRedirect(url: URL): string | undefined {
  const path = Str.replace(/\/$/, "")(url.pathname)
  const suffix = /\.mdx?$/.exec(path)?.[0] ?? ""
  const target = redirects[suffix.length > 0 ? Str.slice(0, -suffix.length)(path) : path]
  return target === undefined ? undefined : `${target}${suffix}${url.search}${url.hash}`
}
