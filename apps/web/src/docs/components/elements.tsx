import { String as Str } from "effect"
import { useMemo, type ComponentPropsWithoutRef, type ReactElement } from "react"
import { renderToString } from "katex"

export const Math = ({ expression }: { readonly expression: string }): ReactElement => {
  const html = useMemo(() => ({ __html: renderToString(expression, { displayMode: true, throwOnError: false }) }), [expression])
  return <div className="docs-math" dangerouslySetInnerHTML={html} />
}

export const Link = ({ href, children, ...props }: ComponentPropsWithoutRef<"a">): ReactElement => {
  const external = href !== undefined && (Str.startsWith("https://")(href) || Str.startsWith("http://")(href))
  return <a href={href} {...props} {...(external ? { rel: "noopener noreferrer", target: "_blank" } : {})}>{children}</a>
}

export const Table = ({ children, ...props }: ComponentPropsWithoutRef<"table">): ReactElement => <div className="docs-table"><table {...props}>{children}</table></div>
