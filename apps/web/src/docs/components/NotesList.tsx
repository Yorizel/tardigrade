import { Link } from "@tanstack/react-router"
import { Array as Arr } from "effect"
import { type ReactElement } from "react"
import { docPages } from "../load"

export const NotesList = (): ReactElement => (
  <ul className="notes-list">
    {Arr.map(Arr.filter(docPages, doc => doc.frontmatter.section === "Notes" && doc.frontmatter.route !== "/notes"), doc => (
      <li key={doc.frontmatter.route}>
        <Link to={doc.frontmatter.route}>{doc.frontmatter.title}</Link>
        <p>{doc.frontmatter.description}</p>
      </li>
    ))}
  </ul>
)
