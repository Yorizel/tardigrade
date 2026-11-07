import { Array as Arr, Option, Order, String as Str } from "effect"

type MediaRange = {
  readonly index: number
  readonly quality: number
  readonly subtype: string
  readonly type: string
}
const mediaRanges = (header: string | null): ReadonlyArray<MediaRange> => Arr.flatMap(Str.split(header ?? "*/*", ","), (entry, index) => {
  const [media, ...parameters] = Str.split(Str.toLowerCase(Str.trim(entry)), ";")
  const [type, subtype, extra] = Str.split(media, "/")
  if (type.length === 0 || subtype === undefined || subtype.length === 0 || extra !== undefined) return []
  const qualityParameter = Option.getOrUndefined(Arr.findFirst(Arr.map(parameters, Str.trim), Str.startsWith("q=")))
  const quality = qualityParameter === undefined ? 1 : Number(Str.slice(2)(qualityParameter))
  return [{ index, quality: Number.isFinite(quality) && quality >= 0 && quality <= 1 ? quality : 0, subtype, type }]
})

const qualityFor = (ranges: ReadonlyArray<MediaRange>, type: string, subtype: string): number => {
  const matches = Arr.filter(ranges, (range) => (range.type === "*" || range.type === type) && (range.subtype === "*" || range.subtype === subtype))
  if (matches.length === 0) return 0
  const specificity = (range: MediaRange): number => Number(range.type !== "*") + Number(range.subtype !== "*")
  return Arr.sort(matches, (left: MediaRange, right: MediaRange) => Order.Number(specificity(right), specificity(left)) || Order.Number(left.index, right.index))[0]?.quality ?? 0
}

export const wantsMarkdown = (header: string | null): boolean | undefined => {
  const ranges = mediaRanges(header)
  const markdown = qualityFor(ranges, "text", "markdown")
  const html = qualityFor(ranges, "text", "html")
  if (markdown === 0 && html === 0) return undefined
  return markdown > html
}
