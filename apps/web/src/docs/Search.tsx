import { Link } from "@tanstack/react-router"
import { MagnifyingGlass, X } from "@phosphor-icons/react"
import { Array as Arr, Option, String as Str } from "effect"
import { useCallback, useEffect, useRef, useState, type ChangeEvent, type KeyboardEvent as ReactKeyboardEvent, type ReactElement } from "react"
import { DEFAULT_DOC_ROUTE, docPages } from "./load"

export const DocSearch = (): ReactElement => {
  const dialog = useRef<HTMLDialogElement>(null)
  const input = useRef<HTMLInputElement>(null)
  const [query, setQuery] = useState("")
  const terms = Arr.filter(Str.split(Str.trim(Str.toLowerCase(query)), /\s+/), term => term.length > 0)
  const matches = Arr.filter(docPages, doc => Arr.every(terms, term => Str.includes(term)(Str.toLowerCase(`${doc.frontmatter.title} ${doc.frontmatter.description} ${doc.markdown}`))))
  const open = useCallback(() => { dialog.current?.showModal(); input.current?.focus() }, [])
  const close = useCallback(() => dialog.current?.close(), [])
  const resetQuery = useCallback(() => setQuery(""), [])
  const changeQuery = useCallback((event: ChangeEvent<HTMLInputElement>) => setQuery(event.target.value), [])
  const navigateResults = useCallback((event: ReactKeyboardEvent<HTMLElement>) => {
    if (event.key === "ArrowDown") {
      event.preventDefault()
      const links = Array.from(dialog.current?.querySelectorAll<HTMLAnchorElement>(".docs-search-results a") ?? [])
      const current = Option.getOrElse(Arr.findFirstIndex(links, link => link === document.activeElement), () => -1)
      links[(current + 1) % links.length]?.focus()
    }
    if (event.key === "ArrowUp") {
      event.preventDefault()
      const links = Array.from(dialog.current?.querySelectorAll<HTMLAnchorElement>(".docs-search-results a") ?? [])
      const current = Option.getOrElse(Arr.findFirstIndex(links, link => link === document.activeElement), () => -1)
      if (current <= 0) input.current?.focus()
      else links[current - 1]?.focus()
    }
    if (event.key === "Enter" && event.target === input.current) { event.preventDefault(); dialog.current?.querySelector<HTMLAnchorElement>(".docs-search-results a")?.click() }
  }, [])
  useEffect(() => {
    const element = dialog.current
    const dismissBackdrop = (event: MouseEvent) => { if (event.target === element) element?.close() }
    const keyboard = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && Str.toLowerCase(event.key) === "k") { event.preventDefault(); open() }
    }
    element?.addEventListener("click", dismissBackdrop)
    document.addEventListener("keydown", keyboard)
    return () => {
      element?.removeEventListener("click", dismissBackdrop)
      document.removeEventListener("keydown", keyboard)
    }
  }, [open])
  return <>
    <button className="docs-search-trigger" type="button" onClick={open}><MagnifyingGlass aria-hidden="true" /><span>Search</span><kbd>⌘ K</kbd></button>
    <dialog className="docs-search-dialog" ref={dialog} onClose={resetQuery} aria-label="Search documentation">
      <div className="docs-search-field"><MagnifyingGlass aria-hidden="true" /><input ref={input} aria-label="Search documentation" placeholder="Search documentation…" value={query} onChange={changeQuery} onKeyDown={navigateResults} /><button type="button" onClick={close} onKeyDown={navigateResults} aria-label="Close search"><X /></button></div>
      <div className="docs-search-results">
        {matches.length === 0 ? <p>No pages match “{query}”.</p> : Arr.map(matches, doc => <Link key={doc.frontmatter.route} to={doc.frontmatter.route === DEFAULT_DOC_ROUTE ? "/" : doc.frontmatter.route} onClick={close} onKeyDown={navigateResults}><span>{doc.frontmatter.route === DEFAULT_DOC_ROUTE ? "Introduction" : doc.frontmatter.title}</span><small>{doc.frontmatter.description}</small></Link>)}
      </div>
      <div className="docs-search-hint"><span>↑ ↓ to navigate</span><span>↵ to open</span><span>esc to close</span></div>
    </dialog>
  </>
}
