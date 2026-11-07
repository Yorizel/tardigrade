import { expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"
import { CompactionMachineDiagram } from "../../notes/diagrams/CompactionMachineDiagram"
import { InterfaceComparisonDiagram } from "../../notes/diagrams/InterfaceComparisonDiagram"

test("exposes native figures with their diagram descriptions", () => {
  const comparison = renderToStaticMarkup(<InterfaceComparisonDiagram />)
  const compaction = renderToStaticMarkup(<CompactionMachineDiagram />)
  expect(comparison).toContain('<figure class="interface-comparison"')
  expect(comparison).toContain("A user interface maps state")
  expect(compaction).toContain('<figure class="compaction-machine-diagram"')
  expect(compaction).toContain("The immutable event log contains a CompactionCompleted checkpoint")
})
