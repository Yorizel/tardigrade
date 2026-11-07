# Third-party notices

## JSON Schema format assertions

The nine JSON Schema format assertions in `packages/domain/src/Output/Output.format.ts` derive from `@cfworker/json-schema` 4.1.1, `src/format.ts`, from [cfworker/cfworker](https://github.com/cfworker/cfworker). The selected date, duration, email, hostname, IPv4, IPv6 and UUID routines retain their source semantics. Time and date-time additionally require complete bounded RFC 3339 offsets and check leap-second placement against UTC. Their integration uses Effect Schema filters; the JSON Schema evaluation engine is not included.

MIT License

Copyright (c) 2020 Jeremy Danyow

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
