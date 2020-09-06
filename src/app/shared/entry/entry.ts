import { JdsDocument } from "../jds-client.service";

export interface Entry extends JdsDocument {
  title?: string;
}
