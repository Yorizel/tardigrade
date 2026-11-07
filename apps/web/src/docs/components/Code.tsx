import { Array as Arr, Option, String as Str } from "effect"
import { Children, isValidElement, useCallback, useId, useLayoutEffect, useMemo, useRef, useState, type ComponentPropsWithoutRef, type CSSProperties, type ReactElement, type ReactNode } from "react"
import { CheckIcon, CopyIcon, useCopy } from "../../ui/copy"
import { ChevronIcon } from "./icons"

const CODE_LINE_HEIGHT = 20

const textFrom = (node: ReactNode): string => {
  if (typeof node === "string" || typeof node === "number") return String(node)
  if (Arr.isArray(node)) return Arr.join(Arr.map(Children.toArray(node), textFrom), "")
  if (isValidElement<{ readonly children?: ReactNode }>(node)) return textFrom(node.props.children)
  return ""
}

const languageOf = (children: ReactNode): string => {
  if (!isValidElement<{ readonly className?: string }>(children)) return "text"
  const match = /(?:^|\s)language-([^\s]+)/.exec(children.props.className ?? "")
  return match?.[1] ?? "text"
}

type CodeProps = ComponentPropsWithoutRef<"pre"> & {
  readonly expanded?: boolean | undefined
  readonly collapsed?: boolean | undefined
  readonly highlight?: number | string | undefined
  readonly variant?: "diagram" | "multi" | "single" | undefined
}

const highlightLines = (value: number | string | undefined): { readonly first: number; readonly count: number } | undefined => {
  const match = /^(\d+)(?:-(\d+))?$/.exec(String(value ?? ""))
  if (match === null) return undefined
  const first = Number(match[1])
  const last = Number(match[2] ?? match[1])
  return first > 0 && last >= first ? { first, count: last - first + 1 } : undefined
}

export const Code = ({ children, title, expanded = false, collapsed = false, highlight, variant = "multi", ...props }: CodeProps): ReactElement => {
  const [copied, copy] = useCopy()
  const [opened, setOpened] = useState(expanded)
  const bodyId = useId()
  const codeRoot = useRef<HTMLDivElement>(null)
  const initiallyScrolled = useRef(false)
  const language = languageOf(children)
  const highlighted = highlightLines(highlight)
  const highlightedLine = highlighted?.first ?? 0
  const highlightedCount = highlighted?.count ?? 0
  const hasHighlight = highlighted !== undefined
  const codeStyle = useMemo<CSSProperties & { "--docs-code-line-height": string; "--docs-highlight-height"?: string; "--docs-highlight-offset"?: string }>(() => ({
    "--docs-code-line-height": `${CODE_LINE_HEIGHT}px`,
    ...(hasHighlight ? {
      "--docs-highlight-height": `${highlightedCount * CODE_LINE_HEIGHT}px`,
      "--docs-highlight-offset": `${(highlightedLine - 1) * CODE_LINE_HEIGHT}px`
    } : {})
  }), [hasHighlight, highlightedCount, highlightedLine])
  const source = Str.trimEnd(textFrom(children))
  const copySource = useCallback(() => { void copy(source) }, [copy, source])
  const toggleOpened = useCallback(() => setOpened(value => !value), [])
  useLayoutEffect(() => {
    if (!hasHighlight) return undefined
    const root = codeRoot.current
    const pre = root?.querySelector("pre")
    const code = pre?.querySelector("code")
    if (root === null || root === undefined || pre === null || pre === undefined || code === null || code === undefined) return undefined
    const position = (): void => {
      const panel = root.closest<HTMLElement>(".docs-code-files-panel")
      if (panel !== null && panel.hidden !== false) return
      const lines = Str.split(code.textContent ?? source, "\n")
      const target = lines[highlightedLine - 1]
      if (target === undefined) return
      const lineStart = Arr.reduce(lines.slice(0, highlightedLine - 1), 0, (length, line) => length + line.length + 1)
      const startOffset = lineStart + globalThis.Math.max(0, Option.getOrElse(Str.search(/\S/)(target), () => -1))
      const endOffset = Arr.join(lines.slice(0, highlightedLine - 1 + highlightedCount), "\n").length
      const range = document.createRange()
      const walker = document.createTreeWalker(code, NodeFilter.SHOW_TEXT)
      let offset = 0
      let started = false
      let node = walker.nextNode()
      while (node !== null) {
        const length = node.textContent?.length ?? 0
        if (!started && startOffset < offset + length) {
          range.setStart(node, startOffset - offset)
          started = true
        }
        if (started && endOffset <= offset + length) {
          range.setEnd(node, endOffset - offset)
          break
        }
        offset += length
        node = walker.nextNode()
      }
      if (!started || node === null) return
      const rects = Arr.filter(Array.from(range.getClientRects()), rect => rect.height > 0)
      const first = rects[0]
      const last = rects[rects.length - 1]
      if (first === undefined || last === undefined) return
      const preRect = pre.getBoundingClientRect()
      const scale = pre.offsetHeight === 0 ? 1 : preRect.height / pre.offsetHeight
      const measuredLineHeight = Number.parseFloat(getComputedStyle(pre).lineHeight)
      const leading = (measuredLineHeight - first.height / scale) / 2
      const top = (first.top - preRect.top) / scale - leading
      const height = (last.bottom - first.top) / scale + leading * 2
      root.style.setProperty("--docs-highlight-position", `${top}px`)
      root.style.setProperty("--docs-highlight-height", `${height}px`)
      if (!initiallyScrolled.current && panel !== null && panel.clientHeight > 0) {
        panel.scrollTop += (first.top - panel.getBoundingClientRect().top) / scale - panel.clientHeight * 0.4
        initiallyScrolled.current = true
      }
    }
    position()
    const observer = new ResizeObserver(position)
    observer.observe(pre)
    const lifetime = new AbortController()
    void document.fonts.ready.then(() => {
      if (!lifetime.signal.aborted) position()
      return undefined
    })
    return () => {
      lifetime.abort()
      observer.disconnect()
    }
  }, [source, hasHighlight, highlightedLine, highlightedCount])
  if (variant === "single") {
    return (
      <div className="install-command docs-code-single" aria-label={`${language} command`}>
        <span aria-hidden="true">$</span>
        {children}
        <button type="button" aria-label={copied ? "Code copied" : "Copy code"} title={copied ? "Copied" : "Copy code"} onClick={copySource}>
          {copied ? <CheckIcon /> : <CopyIcon />}
        </button>
      </div>
    )
  }
  if (variant === "diagram") {
    return <div className="docs-text-diagram"><pre {...props}>{children}</pre></div>
  }
  return (
    <div ref={codeRoot} className="concept-code docs-code" data-expanded={collapsed ? opened : expanded} data-collapsible={collapsed || undefined} data-highlight={hasHighlight ? "true" : undefined} style={codeStyle}>
      <div className="docs-code-header"><span>{title ?? (language === "ts" || language === "typescript" ? "TypeScript" : language)}</span></div>
      <div className="docs-code-body" id={bodyId}><pre {...props}>{children}</pre></div>
      {collapsed && <button className="docs-code-toggle" type="button" aria-expanded={opened} aria-controls={bodyId} onClick={toggleOpened}>
        {opened ? "Collapse code" : "Expand code"}<ChevronIcon />
      </button>}
      <button type="button" aria-label={copied ? "Code copied" : "Copy code"} onClick={copySource}>
        {copied ? <CheckIcon /> : <CopyIcon />}
      </button>
    </div>
  )
}

export const InlineCode = ({ className, ...props }: ComponentPropsWithoutRef<"code">): ReactElement => {
  const block = className === undefined ? false : Arr.some(Str.split(className, " "), (name) => name === "hljs" || Str.startsWith("language-")(name))
  return <code className={Str.trim(`${className ?? ""}${block ? "" : " docs-inline-code"}`)} {...props} />
}
