import { Array as Arr, Option, Record, Schema, String as Str } from "effect"
import type { SchemaAST } from "effect"

const DATE = /^(\d\d\d\d)-(\d\d)-(\d\d)$/
const DAYS = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
const TIME = /^(\d\d):(\d\d):(\d\d)(\.\d+)?(z|[+-]\d\d(?::?\d\d)?)?$/i
const HOSTNAME = /^(?=.{1,253}\.?$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-0-9a-z]{0,61}[0-9a-z])?)*\.?$/i
const UUID = /^(?:urn:uuid:)?[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i
const IPV4 = /^(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)$/
const IPV6 =
  /^((([0-9a-f]{1,4}:){7}([0-9a-f]{1,4}|:))|(([0-9a-f]{1,4}:){6}(:[0-9a-f]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9a-f]{1,4}:){5}(((:[0-9a-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9a-f]{1,4}:){4}(((:[0-9a-f]{1,4}){1,3})|((:[0-9a-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9a-f]{1,4}:){3}(((:[0-9a-f]{1,4}){1,4})|((:[0-9a-f]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9a-f]{1,4}:){2}(((:[0-9a-f]{1,4}){1,5})|((:[0-9a-f]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9a-f]{1,4}:){1}(((:[0-9a-f]{1,4}){1,6})|((:[0-9a-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9a-f]{1,4}){1,7})|((:[0-9a-f]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))$/i;
const DURATION = (input: string) =>
  input.length > 1 &&
  input.length < 80 &&
  (/^P\d+([.,]\d+)?W$/.test(input) ||
    (/^P[\dYMDTHS]*(\d[.,]\d+)?[YMDHS]$/.test(input) &&
      /^P([.,\d]+Y)?([.,\d]+M)?([.,\d]+D)?(T([.,\d]+H)?([.,\d]+M)?([.,\d]+S)?)?$/.test(input)))

const EMAIL = (input: string) => {
  if (input[0] === '"') return false
  const [name, host, ...rest] = Str.split("@")(input)
  if (name === undefined || name === "" || host === undefined || host === "" || rest.length !== 0 || name.length > 64 || host.length > 253) return false
  if (name[0] === "." || Str.endsWith(".")(name) || Str.includes("..")(name)) return false
  if (!/^[a-z0-9.-]+$/i.test(host) || !/^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+$/i.test(name)) return false
  return Arr.every(Str.split(".")(host), part => /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/i.test(part))
}

const isLeapYear = (year: number) => year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)

const date = (input: string) => {
  const matched = Str.match(DATE)(input)
  if (Option.isNone(matched)) return false
  const matches = matched.value
  const year = Number(matches[1])
  const month = Number(matches[2])
  const day = Number(matches[3])
  if (month < 1 || month > 12 || day < 1) return false
  if (month === 2 && isLeapYear(year)) return day <= 29
  const maximumDay = DAYS[month]
  return maximumDay !== undefined && day <= maximumDay
}

const time = (input: string, full: boolean) => {
  const matched = Str.match(TIME)(input)
  if (Option.isNone(matched)) return false
  const matches = matched.value
  const hour = Number(matches[1])
  const minute = Number(matches[2])
  const second = Number(matches[3])
  const timeZone = Boolean(matches[5])
  return ((hour <= 23 && minute <= 59 && second <= 59) || (hour === 23 && minute === 59 && second === 60)) && (!full || timeZone)
}

const dateTime = (input: string) => {
  const parts = Str.split(/t|\s/i)(input)
  if (parts.length !== 2) return false
  const [day, clock] = parts
  return day !== undefined && clock !== undefined && date(day) && time(clock, true)
}

export const OutputFormatChecks = Record.map({
  "date-time": dateTime,
  time: (input: string) => time(input, false),
  date,
  duration: DURATION,
  email: EMAIL,
  hostname: (input: string) => HOSTNAME.test(input),
  ipv4: (input: string) => IPV4.test(input),
  ipv6: (input: string) => IPV6.test(input),
  uuid: (input: string) => UUID.test(input),
}, (predicate, format): SchemaAST.Filter<string> => Schema.makeFilter(input => {
  if (!predicate(input)) return `must match JSON Schema format "${format}"`
  return undefined
}))
