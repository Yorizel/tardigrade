import { NotesList } from "./NotesList"
import { AuthorAvatar } from "../../notes/AuthorAvatar"
import { Dialogue, Socrates, You } from "../../notes/Dialogue"
import { AgentViewDiagram } from "../../notes/diagrams/AgentViewDiagram"
import { CaveDiagram } from "../../notes/diagrams/CaveDiagram"
import { FlyingTardie } from "../../notes/diagrams/FlyingTardie"
import { BehaviorTrajectoryDiagram } from "../../notes/diagrams/BehaviorTrajectoryDiagram"
import { CompactionMachineDiagram } from "../../notes/diagrams/CompactionMachineDiagram"
import { HarnessDiagram } from "../../notes/diagrams/HarnessDiagram"
import { InterfaceComparisonDiagram } from "../../notes/diagrams/InterfaceComparisonDiagram"
import { TrajectoryBranchesDiagram } from "../../notes/diagrams/TrajectoryBranchesDiagram"
import { Code, InlineCode } from "./Code"
import { Link, Math, Table } from "./elements"

export const mdxComponents = {
  NotesList,
  AuthorAvatar,
  Dialogue,
  Socrates,
  You,
  AgentViewDiagram,
  CaveDiagram,
  FlyingTardie,
  BehaviorTrajectoryDiagram,
  CompactionMachineDiagram,
  HarnessDiagram,
  InterfaceComparisonDiagram,
  Math,
  TrajectoryBranchesDiagram,
  a: Link,
  code: InlineCode,
  table: Table,
  pre: Code
}
