import type { ReactElement } from "react"

const ResourceCell = ({ x, y, kind, filled, spill = false }: { readonly x: number; readonly y: number; readonly kind: "water" | "clips"; readonly filled: boolean; readonly spill?: boolean }): ReactElement => (
  <g transform={`translate(${x} ${y})`}>
    <rect className={spill ? "factory-resource-spill" : "factory-resource-cell"} data-filled={filled} width="10" height="10" />
    <g className="factory-resource-symbol" data-filled={filled} transform="translate(1 1) scale(.25)">
      {kind === "water" ? <path d="M16 3C13 9 6 15 6 21a10 10 0 0 0 20 0C26 15 19 9 16 3Z" /> : <path d="m13 23 10-10a5 5 0 0 0-7-7L5 17a8 8 0 0 0 11 11L27 17M9 21 20 10a2 2 0 0 1 3 3L13 23" />}
    </g>
  </g>
)

export const ResourceGrid = ({ water, clips, spill = Math.max(0, water - 10), capacity = 10, clipGoal = 1_000 }: { readonly water: number; readonly clips: number; readonly spill?: number; readonly capacity?: number; readonly clipGoal?: number }): ReactElement => (
  <g className="factory-resource-grid" role="img" aria-label={`${Math.min(water, capacity)} of ${capacity} water units, ${spill} units spilled, ${clips} of ${clipGoal} paperclips`}>
    <g className="factory-resource-water" data-overflow={spill > 0}>
    <text className="factory-resource-label" x="0" y="15">Water</text>
    <g transform="translate(36 0)">
    {Array.from({ length: capacity }, (_, index) => <ResourceCell key={index} kind="water" filled={index < Math.min(water, capacity)} x={(index % 5) * 13} y={Math.floor(index / 5) * 13} />)}
    {Array.from({ length: spill }, (_, index) => <ResourceCell key={`spill-${index}`} kind="water" filled spill x={75 + (index % 2) * 13} y={Math.floor(index / 2) * 13} />)}
    {spill > 0 && <path className="factory-resource-divider" d="M68-2v27" />}
    </g>
    </g>
    <text className="factory-resource-label" x="0" y="48">Clips</text>
    <g transform="translate(36 0)">
    {Array.from({ length: 10 }, (_, index) => <ResourceCell key={`clips-${index}`} kind="clips" filled={index < clips / (clipGoal / 10)} x={(index % 5) * 13} y={33 + Math.floor(index / 5) * 13} />)}
    </g>
  </g>
)
