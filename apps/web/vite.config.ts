import mdx from "@mdx-js/rollup"
import { layer as fileSystemLayer } from "@effect/platform-bun/BunFileSystem"
import { tanstackStart } from "@tanstack/react-start/plugin/vite"
import { Effect, FileSystem, Schema, String as Str } from "effect"
import { fileURLToPath } from "node:url"
import { nitro } from "nitro/vite"
import { highlight } from "./plugins/highlight"
import rehypeMdxCodeProps from "rehype-mdx-code-props"
import rehypeSlug from "rehype-slug"
import { defineConfig, type Plugin } from "vite"
import react from "@vitejs/plugin-react"
import remarkFrontmatter from "remark-frontmatter"
import remarkGfm from "remark-gfm"
import remarkMdxFrontmatter from "remark-mdx-frontmatter"

const prerenderedPages = new Set<string>()
const requiredPrerenderPages = new Set<string>()

const DOC_SOURCE_QUERY = "?doc-source"
const DOC_SOURCE_PREFIX = "\0doc-source:"
const sourceSchema = Schema.fromJsonString(Schema.String)

const docSource = (): Plugin => ({
  name: "doc-source",
  enforce: "pre",
  resolveId(source, importer, options) {
    if (!Str.endsWith(DOC_SOURCE_QUERY)(source)) return undefined
    return this.resolve(Str.slice(0, -DOC_SOURCE_QUERY.length)(source), importer, { ...options, skipSelf: true })
      .then(resolved => resolved === null ? undefined : `${DOC_SOURCE_PREFIX}${resolved.id}.ts`)
  },
  load(id) {
    if (!Str.startsWith(DOC_SOURCE_PREFIX)(id) || !Str.endsWith(".ts")(id)) return undefined
    const filename = Str.slice(DOC_SOURCE_PREFIX.length, -3)(id)
    this.addWatchFile(filename)
    return Effect.runPromise(Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const source = yield* fs.readFileString(filename)
      const encoded = yield* Schema.encodeEffect(sourceSchema)(source)
      return `export default ${encoded}`
    }).pipe(Effect.provide(fileSystemLayer)))
  }
})

export default defineConfig({
  resolve: { alias: { "@docs": fileURLToPath(new URL("../../docs", import.meta.url)), "@notes": fileURLToPath(new URL("../../notes", import.meta.url)) } },
  plugins: [
    docSource(),
    {
      enforce: "pre",
      ...mdx({
        providerImportSource: "@mdx-js/react",
        rehypePlugins: [rehypeSlug, highlight, rehypeMdxCodeProps],
        remarkPlugins: [remarkGfm, remarkFrontmatter, [remarkMdxFrontmatter, { name: "frontmatter" }]]
      })
    },
    tanstackStart({
      prerender: {
        enabled: true,
        filter: ({ path }) => {
          if (path === "/console" || Str.startsWith("/docs")(path)) return false
          requiredPrerenderPages.add(path)
          return true
        },
        onSuccess: ({ page }) => { prerenderedPages.add(page.path) }
      }
    }),
    nitro(),
    {
      name: "configure-prerender-preview",
      enforce: "post",
      apply: (_config, env) => env.isPreview,
      config: () => ({ preview: { port: 0, host: "127.0.0.1" } })
    },
    {
      name: "verify-prerender",
      enforce: "post",
      buildApp: {
        order: "post",
        handler() {
          if (!requiredPrerenderPages.has("/")) throw new Error("Required prerender was not scheduled: /")
          for (const path of requiredPrerenderPages) {
            if (!prerenderedPages.has(path)) throw new Error(`Required prerender failed: ${path}`)
          }
        }
      }
    },
    react({ include: /\.(?:js|jsx|mdx|ts|tsx)$/ })
  ]
})
