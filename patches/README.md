# Effect dependency patch

`package.json` owns the patched dependency and its exact patch path. Bun applies the patch during installation. The patch adds an optional `formatChecks` map to Effect's JSON Schema importer; each provided native Schema filter composes with the imported string schema. Without a supplied map, formats keep their annotation-only behavior. The change covers runtime distribution, source and declarations.

`Output.format.ts` supplies the nine allowlisted format assertions. Native importer tests cover nested objects, unions, format-only schemas, the unchanged default and inherited map names. Output tests compare the unchanged assertions with the existing validator and exercise nested composition. Time and date-time require a complete RFC 3339 offset, with bounded offset components and UTC-relative leap-second placement. The application rejects unknown format names at declaration decoding.

Published artifacts carry the referenced patch files. Generated native projects copy those files and declare their own `patchedDependencies`, because dependency patches do not propagate transitively. A source checkout or generated project must use its own manifest's patch metadata when installing dependencies.

Domain commands use native `Schema.TaggedError` classes as rejections, as supported by the Domain contract. No patch to `@alchemy.run/domain` is required.
