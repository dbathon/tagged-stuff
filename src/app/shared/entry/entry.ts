import { JdsDocument } from "../jds-client";

export interface Entry extends JdsDocument {
  title?: string;
}
