import { docHead } from "../docs/head"
import { docsRedirect } from "../docs/redirects"
import { createFileRoute, notFound, redirect } from "@tanstack/react-router"
import type { ReactElement } from "react"

import { DocsPage } from "../docs/Docs"
import { docAt } from "../docs/load"
import { docsResponse } from "../docs/response.server"

const pathnameOf = (splat: string | undefined): string => `/docs/${splat ?? ""}`

export const Route = createFileRoute("/docs/$")({
  server: { handlers: { GET: ({ request, next }) => docsResponse({ request }) ?? next(), HEAD: ({ request, next }) => docsResponse({ request }) ?? next() } },
  beforeLoad: ({ params: { _splat: splat }, location }) => {
    const target = docsRedirect(new URL(location.href, "https://tardigrade.sh"))
    if (target !== undefined) throw redirect({ href: target, statusCode: 301 })
    if (docAt(pathnameOf(splat)) === undefined) throw notFound()
  },
  component: DocRoute,
  head: ({ params: { _splat: splat } }) => docHead(pathnameOf(splat))
})

function DocRoute(): ReactElement {
  const { _splat: splat } = Route.useParams()
  return <DocsPage pathname={pathnameOf(splat)} />
}
