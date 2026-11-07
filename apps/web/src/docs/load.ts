import { Array as Arr, Option, Order, Schema, String as Str } from "effect"
import type { ComponentType } from "react"
import Welcome, { frontmatter as welcomeFrontmatter } from "@docs/introduction.mdx"
import welcomeMarkdown from "@docs/introduction.mdx?doc-source"
import Architecture, { frontmatter as architectureFrontmatter } from "@docs/architecture.mdx"
import architectureMarkdown from "@docs/architecture.mdx?doc-source"
import Alchemy, { frontmatter as alchemyFrontmatter } from "@docs/platforms/alchemy.mdx"
import alchemyMarkdown from "@docs/platforms/alchemy.mdx?doc-source"
import NotesIndex, { frontmatter as notesIndexFrontmatter } from "@docs/notes/index.mdx"
import notesIndexMarkdown from "@docs/notes/index.mdx?doc-source"
import Why, { frontmatter as whyFrontmatter } from "@docs/notes/why-tardigrade.mdx"
import whyMarkdown from "@docs/notes/why-tardigrade.mdx?doc-source"
import FormsAndShadows, { frontmatter as formsAndShadowsNote } from "@notes/telling-forms-from-their-shadows.mdx"
import formsAndShadowsMarkdown from "@notes/telling-forms-from-their-shadows.mdx?doc-source"

type DocFrontmatter = {
  readonly title: string
  readonly description: string
  readonly route: string
  readonly section: string
  readonly sectionOrder: number
  readonly order: number
  readonly draft?: boolean | undefined
  readonly deprecated?: boolean | undefined
  readonly articleClass?: string | undefined
  readonly hideDescription?: boolean | undefined
  readonly socialImage?: string | undefined
  readonly socialImageAlt?: string | undefined
}

export type Doc = {
  readonly Content: ComponentType
  readonly frontmatter: DocFrontmatter
  readonly markdown: string
  readonly source: string
}

type DocModule = {
  readonly default: ComponentType
  readonly frontmatter: unknown
  readonly markdown: string
  readonly source: string
}

const modules: ReadonlyArray<DocModule> = [
  { default: Welcome, frontmatter: welcomeFrontmatter, markdown: welcomeMarkdown, source: "introduction.mdx" },
  { default: Architecture, frontmatter: architectureFrontmatter, markdown: architectureMarkdown, source: "architecture.mdx" },
  { default: Alchemy, frontmatter: alchemyFrontmatter, markdown: alchemyMarkdown, source: "platforms/alchemy.mdx" },
  { default: NotesIndex, frontmatter: notesIndexFrontmatter, markdown: notesIndexMarkdown, source: "notes/index.mdx" },
  { default: Why, frontmatter: whyFrontmatter, markdown: whyMarkdown, source: "notes/why-tardigrade.mdx" },
  { default: FormsAndShadows, frontmatter: formsAndShadowsNote, markdown: formsAndShadowsMarkdown, source: "../notes/telling-forms-from-their-shadows.mdx" },
]

const frontmatterRecord = Schema.Record(Schema.String, Schema.Unknown)

const stringField = (value: Record<string, unknown>, field: string, source: string): string => {
  const found = value[field]
  if (typeof found !== "string" || Str.trim(found).length === 0) throw new Error(`${source}: frontmatter.${field} must be a non-empty string`)
  return found
}

const numberField = (value: Record<string, unknown>, field: string, source: string): number => {
  const found = value[field]
  if (typeof found !== "number" || !Number.isFinite(found)) throw new Error(`${source}: frontmatter.${field} must be a number`)
  return found
}

const readFrontmatter = (value: unknown, source: string): DocFrontmatter => {
  if (Arr.isArray(value) || !Schema.is(frontmatterRecord)(value)) throw new Error(`${source}: frontmatter must be an object`)
  const fields = value
  const route = stringField(fields, "route", source)
  if (!Str.startsWith("/")(route) || (route.length > 1 && Str.endsWith("/")(route))) throw new Error(`${source}: frontmatter.route must start with / and omit a trailing /`)
  const articleClass = fields.articleClass
  if (articleClass !== undefined && typeof articleClass !== "string") throw new Error(`${source}: frontmatter.articleClass must be a string`)
  const draft = fields.draft
  if (draft !== undefined && typeof draft !== "boolean") throw new Error(`${source}: frontmatter.draft must be a boolean`)
  const deprecated = fields.deprecated
  if (deprecated !== undefined && typeof deprecated !== "boolean") throw new Error(`${source}: frontmatter.deprecated must be a boolean`)
  const hideDescription = fields.hideDescription
  if (hideDescription !== undefined && typeof hideDescription !== "boolean") throw new Error(`${source}: frontmatter.hideDescription must be a boolean`)
  return {
    title: stringField(fields, "title", source),
    description: stringField(fields, "description", source),
    route,
    section: stringField(fields, "section", source),
    sectionOrder: numberField(fields, "sectionOrder", source),
    order: numberField(fields, "order", source),
    draft,
    deprecated,
    articleClass,
    hideDescription,
    socialImage: fields.socialImage === undefined ? undefined : stringField(fields, "socialImage", source),
    socialImageAlt: fields.socialImageAlt === undefined ? undefined : stringField(fields, "socialImageAlt", source)
  }
}

const docs: ReadonlyArray<Doc> = Arr.sort(Arr.map(modules, (module) => {
    const frontmatter = readFrontmatter(module.frontmatter, module.source)
    if (frontmatter.draft === true) throw new Error(`${module.source}: draft docs cannot enter the public registry`)
    return { Content: module.default, frontmatter, markdown: module.markdown, source: module.source }
  }), (left: Doc, right: Doc) => Order.Number(left.frontmatter.sectionOrder, right.frontmatter.sectionOrder) || Order.Number(left.frontmatter.order, right.frontmatter.order))

export const docPages = docs

const routes = new Set<string>()
for (const doc of docs) {
  if (routes.has(doc.frontmatter.route)) throw new Error(`${doc.source}: duplicate docs route ${doc.frontmatter.route}`)
  routes.add(doc.frontmatter.route)
}

export const docSections: ReadonlyArray<readonly [string, ReadonlyArray<Doc>]> = [...Arr.reduce(docs, new Map<string, Array<Doc>>(), (grouped, doc) => {
  const pages = grouped.get(doc.frontmatter.section) ?? []
  pages.push(doc)
  grouped.set(doc.frontmatter.section, pages)
  return grouped
}).entries()]

export const DEFAULT_DOC_ROUTE = "/docs/welcome"

export const docAt = (pathname: string): Doc | undefined =>
  Option.getOrUndefined(Arr.findFirst(docs, (doc) => doc.frontmatter.route === (pathname === "/" || pathname === "/docs" ? DEFAULT_DOC_ROUTE : Str.replace(/\/$/, "")(pathname))))
