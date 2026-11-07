import { codeToTokens, isSupportedLanguage } from "@pierre/highlights"
import dark from "@pierre/highlights/themes/pierre-dark"
import light from "@pierre/highlights/themes/pierre-light"
import { Array as Arr, Option, Record as Rec, String as Str } from "effect"

type Node = {
  type: string
  tagName?: string
  properties?: Record<string, unknown>
  value?: string
  children?: Node[]
}

export const highlight = () => (tree: Node): void => {
  const visit = (node: Node): void => {
    if (node.tagName === "pre") {
      const code = node.children === undefined ? undefined : Option.getOrUndefined(Arr.findFirst(node.children, child => child.tagName === "code"))
      const classes = code?.properties?.className
      const languageClass = Arr.isArray(classes) ? Option.getOrUndefined(Arr.findFirst(classes, (value): value is string => typeof value === "string" && Str.startsWith("language-")(value))) : undefined
      const language = languageClass === undefined ? undefined : Str.slice(9)(languageClass)
      if (code !== undefined && language !== undefined && isSupportedLanguage(language)) {
        const source = code.children === undefined ? "" : Arr.join(Arr.map(code.children, child => child.value ?? ""), "")
        const result = codeToTokens(source, { lang: language, themes: { light, dark }, defaultColor: "light-dark()" })
        code.children = Arr.flatMap(result.tokens, (line, index): Node[] => {
          const nodes: Node[] = []
          if (index !== 0) nodes.push({ type: "text", value: "\n" })
          nodes.push(...Arr.map(line, token => ({ type: "element", tagName: "span", properties: { style: Arr.join(Arr.map(Rec.toEntries(token.htmlStyle ?? {}), ([key, value]) => `${key}:${value}`), ";") }, children: [{ type: "text", value: token.content }] })))
          return nodes
        })
      }
      return
    }
    if (node.children !== undefined) Arr.forEach(node.children, visit)
  }
  visit(tree)
}
