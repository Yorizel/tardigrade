import { Array as Arr } from "effect"
import type { ReactElement } from "react"

type PostalStampProps = {
  readonly className: string
  readonly height: number
  readonly id: string
  readonly width: number
  readonly x: number
  readonly y: number
}

const positions = (start: number, length: number, spacing: number): ReadonlyArray<number> => {
  const count = Math.max(1, Math.floor(length / spacing))
  const gap = length / count
  return Array.from({ length: count }, (_, index) => start + gap * (index + 0.5))
}

export const PostalStamp = ({ className, height, id, width, x, y }: PostalStampProps): ReactElement => {
  const horizontal = positions(x, width, 10)
  const vertical = positions(y, height, 10)
  const radius = 2.4
  const outline = Arr.join([
    `M${x} ${y}`,
    ...Arr.map(horizontal, position => `H${position - radius}A${radius} ${radius} 0 0 0 ${position + radius} ${y}`),
    `H${x + width}`,
    ...Arr.map(vertical, position => `V${position - radius}A${radius} ${radius} 0 0 0 ${x + width} ${position + radius}`),
    `V${y + height}`,
    ...Arr.map(Arr.reverse(horizontal), position => `H${position + radius}A${radius} ${radius} 0 0 0 ${position - radius} ${y + height}`),
    `H${x}`,
    ...Arr.map(Arr.reverse(vertical), position => `V${position + radius}A${radius} ${radius} 0 0 0 ${x} ${position - radius}`),
    "Z"
  ], " ")

  return (
    <g className={className} aria-hidden="true">
      <mask id={id} x={x - 4} y={y - 4} width={width + 8} height={height + 8} maskUnits="userSpaceOnUse">
        <rect x={x} y={y} width={width} height={height} fill="white" />
        {Arr.flatMap(horizontal, (position) => [
          <circle cx={position} cy={y} r="2.4" fill="black" key={`top-${position}`} />,
          <circle cx={position} cy={y + height} r="2.4" fill="black" key={`bottom-${position}`} />
        ])}
        {Arr.flatMap(vertical, (position) => [
          <circle cx={x} cy={position} r="2.4" fill="black" key={`left-${position}`} />,
          <circle cx={x + width} cy={position} r="2.4" fill="black" key={`right-${position}`} />
        ])}
      </mask>
      <rect className="postal-stamp-fill" x={x} y={y} width={width} height={height} mask={`url(#${id})`} />
      <path className="postal-stamp-outline" d={outline} fill="none" />
      <rect className="postal-stamp-inset" x={x + 5} y={y + 5} width={width - 10} height={height - 10} />
    </g>
  )
}
