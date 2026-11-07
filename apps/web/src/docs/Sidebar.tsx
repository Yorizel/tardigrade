import { Link } from "@tanstack/react-router"
import { ArrowLeft, GithubLogo, List, Notebook, X } from "@phosphor-icons/react"
import { Array as Arr, String as Str } from "effect"
import { useCallback, useEffect, useRef, useState, type MouseEvent, type ReactElement, type ReactNode } from "react"
import { ThemeToggle } from "../site/ThemeToggle"
import { DocSearch } from "./Search"
import { DEFAULT_DOC_ROUTE, docSections, type Doc } from "./load"

const HIDDEN_SECTIONS = new Set(["Guides", "References", "Examples", "Migration"])

const DocLink = ({ doc, current, className, children, onClick }: {
  readonly doc: Doc
  readonly current?: Doc
  readonly className?: string
  readonly children?: ReactNode
  readonly onClick?: () => void
}): ReactElement => {
  const props = { className, onClick, "aria-current": doc === current ? "page" as const : undefined }
  return doc.frontmatter.route === DEFAULT_DOC_ROUTE
    ? <Link to="/" {...props}>{children ?? "Introduction"}</Link>
    : <Link to={doc.frontmatter.route} {...props}>{children ?? doc.frontmatter.title}</Link>
}

export const Sidebar = ({ current }: { readonly current: Doc }): ReactElement => {
  const [open, setOpen] = useState(false)
  const [activeSection, setActiveSection] = useState<string>()
  const navigation = useRef<HTMLElement>(null)
  const closeNavigation = useCallback(() => setOpen(false), [])
  const toggleNavigation = useCallback(() => setOpen(value => !value), [])
  const selectSection = useCallback((event: MouseEvent<HTMLAnchorElement>) => {
    setActiveSection(decodeURIComponent(Str.slice(1)(event.currentTarget.hash)))
    setOpen(false)
  }, [])
  const markdown = Str.replace(/```[\s\S]*?```/g, "")(current.markdown)
  const headingPattern = /^## /m.test(markdown) ? /^## (.+)$/gm : /^### (.+)$/gm
  const sectionLinks = Arr.map([...Str.matchAll(headingPattern)(markdown)], ([, title]) => {
    const hash = Str.replace(/ /g, "-")(Str.replace(/[^a-z0-9 ]/g, "")(Str.toLowerCase(title)))
    return <a key={hash} href={`#${hash}`} aria-current={activeSection === hash ? "location" : undefined} onClick={selectSection}>{title}</a>
  })
  useEffect(() => {
    const article = document.querySelector<HTMLElement>(".guide-article")
    if (article?.dataset.docRoute !== current.frontmatter.route) return undefined
    const headings = Arr.filter(Arr.map(Array.from(navigation.current?.querySelectorAll<HTMLAnchorElement>('a[href*="#"]') ?? []),
      link => document.getElementById(decodeURIComponent(Str.slice(1)(new URL(link.href).hash)))),
      (heading): heading is HTMLElement => heading !== null && article.contains(heading))
    const update = () => {
      const atBottom = window.scrollY + window.innerHeight >= document.documentElement.scrollHeight - 2
      const reached = Arr.filter(headings, heading => heading.getBoundingClientRect().top <= 48)
      setActiveSection((atBottom ? headings.at(-1) : reached.at(-1) ?? headings[0])?.id)
    }
    const observer = new IntersectionObserver(update, { rootMargin: "-48px 0px 0px 0px" })
    Arr.forEach(headings, heading => observer.observe(heading))
    const onScroll = () => {
      if (window.scrollY + window.innerHeight >= document.documentElement.scrollHeight - 2) update()
    }
    update()
    window.addEventListener("scroll", onScroll, { passive: true })
    window.addEventListener("resize", update)
    return () => {
      observer.disconnect()
      window.removeEventListener("scroll", onScroll)
      window.removeEventListener("resize", update)
    }
  }, [current])
  const note = current.frontmatter.section === "Notes" && current.frontmatter.route !== "/notes"
  const noteLinks = note ? Arr.map([...Str.matchAll(/^(##|###) (.+)$/gm)(markdown)], ([, level, title]) => {
    const hash = Str.replace(/ /g, "-")(Str.replace(/[^\p{L}\p{M}\p{N}\p{Pc} -]/gu, "")(Str.toLowerCase(title)))
    return <a key={hash} href={`#${hash}`} data-depth={level.length} aria-current={activeSection === hash ? "location" : undefined} onClick={selectSection}>{Str.replace(/[`~*_]/g, "")(title)}</a>
  }) : []
  return <aside className="guide-sidebar" data-open={open}>
    <div className="docs-sidebar-brand"><Link to="/" className="brand" aria-label="Tardigrade introduction"><span>Tardigrade</span></Link><button className="docs-menu-toggle" type="button" aria-label={open ? "Close navigation" : "Open navigation"} aria-expanded={open} aria-controls="docs-navigation" onClick={toggleNavigation}>{open ? <X /> : <List />}</button></div>
    <div className="docs-sidebar-tools">
      <a href="https://github.com/clavia-labs/tardigrade" target="_blank" rel="noreferrer" aria-label="GitHub" title="GitHub"><GithubLogo aria-hidden="true" /></a>
      <Link to="/notes" aria-label="Field notes" title="Field notes"><Notebook aria-hidden="true" /></Link>
      <ThemeToggle />
    </div>
    <DocSearch />
    <nav ref={navigation} id="docs-navigation" className="docs-navigation" aria-label="Documentation">
      {note ? <div className="docs-note-nav">
        <Link to="/notes" className="docs-note-back" onClick={closeNavigation}><ArrowLeft aria-hidden="true" />Notes</Link>
        <div className="docs-group-outline">{noteLinks}</div>
      </div> : Arr.map(Arr.filter(docSections, ([section]) => !HIDDEN_SECTIONS.has(section)), ([section, pages]) => section === "Start Here" ? <div className="docs-introduction-nav" key={section}>
        <Link to="/" className="docs-introduction-title" aria-current={current.frontmatter.route === DEFAULT_DOC_ROUTE ? "page" : undefined}>Introduction</Link>
        {current.frontmatter.route === DEFAULT_DOC_ROUTE && <div className="docs-introduction-outline">
          <a href="#how-it-works" aria-current={activeSection === "how-it-works" ? "location" : undefined} onClick={selectSection}>How it works</a>
          <a href="#why-tardigrade" aria-current={activeSection === "why-tardigrade" ? "location" : undefined} onClick={selectSection}>Why Tardigrade?</a>
          <a href="#use-tardigrade" aria-current={activeSection === "use-tardigrade" ? "location" : undefined} onClick={selectSection}>Use Tardigrade</a>
          <a href="#run-tardigrade" aria-current={activeSection === "run-tardigrade" ? "location" : undefined} onClick={selectSection}>Run Tardigrade</a>
          <a href="#explore" aria-current={activeSection === "explore" ? "location" : undefined} onClick={selectSection}>Explore</a>
        </div>}
        {Arr.map(Arr.filter(pages, page => page.frontmatter.route !== DEFAULT_DOC_ROUTE), page => <DocLink key={page.frontmatter.route} doc={page} current={current} className="docs-nav-section" onClick={closeNavigation} />)}
      </div> : <div className="docs-nav-group" key={section}>
        <DocLink doc={pages[0]} current={pages.includes(current) ? pages[0] : undefined} className="docs-nav-section" onClick={closeNavigation}>{section}</DocLink>
        {current.frontmatter.section === section && section !== "Notes" && pages.length === 1 && sectionLinks.length > 0 && <div className="docs-group-outline">{sectionLinks}</div>}
        {current.frontmatter.section === section && section !== "Notes" && pages.length > 1 && <div className="docs-group-outline">{Arr.map(Arr.filter(pages, page => page.frontmatter.route !== "/docs/verifying-actors"), page => <div className="docs-nav-page" key={page.frontmatter.route}><DocLink doc={page} current={current} onClick={closeNavigation}>{page.frontmatter.title}{page.frontmatter.deprecated === true ? <span className="guide-deprecated">Deprecated</span> : null}</DocLink>{page === current && section !== "Concepts" && <div className="docs-page-outline">{sectionLinks}</div>}</div>)}</div>}
      </div>)}
      {!note && <div className="docs-nav-group"><a className="docs-nav-section" href="https://github.com/clavia-labs/tardigrade" target="_blank" rel="noreferrer" onClick={closeNavigation}>GitHub</a></div>}
    </nav>

  </aside>
}
