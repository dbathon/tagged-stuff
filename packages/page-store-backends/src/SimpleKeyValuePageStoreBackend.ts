import { fromByteArray, toByteArray } from "base64-js";
import type { BackendIndexPage, BackendPage, BackendPageIdentifier } from "page-store";
import { AbstractPageStoreBackend } from "./AbstractPageStoreBackend";
import { assert } from "shared-util";

/*
The following talks to a backend that has a single URL that accepts POST requests.

The idea is that this backend can easily be implemented with a serverless function like AWS Lambda, Azure Functions,
Cloudflare Workers etc.. It would be kind of nice to have a more RESTful API, but that requires very many CORS
preflight requests and doing it this way also makes it easier to implement.
*/

type Entry = {
  version: string;
  data: string;
};

type ListKeysResponse = {
  keys: string[];
};

const INDEX_PAGE_KEY = "index";

function toSortableNumberString(positiveNumber: number): string {
  assert(positiveNumber >= 0);
  const numberString = positiveNumber.toString(10);
  // prefix the result with its length (in base36), so that alphanumeric sorting works
  return numberString.length.toString(36) + numberString;
}

function toKey(identifier: BackendPageIdentifier): string {
  return toSortableNumberString(identifier.pageNumber) + "_" + toSortableNumberString(identifier.transactionId);
}

export class SimpleKeyValuePageStoreBackend extends AbstractPageStoreBackend {
  constructor(
    readonly baseUrl: string,
    private readonly authorizationHeaderValue: string | undefined,
  ) {
    super();
  }

  private async execute<T = undefined>(
    operation: string,
    body: Record<string, string | undefined>,
  ): Promise<T | 204 | 404 | 409> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (this.authorizationHeaderValue) {
      headers["Authorization"] = this.authorizationHeaderValue;
    }
    const response = await fetch(this.baseUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({ operation, ...body }),
    });
    const status = response.status;
    if (status === 204 || status === 404 || status === 409) {
      return status;
    }
    if (status >= 200 && status < 300) {
      return response.json();
    }
    throw new Error("request failed with status " + status);
  }

  protected async fetchPage<T>(key: string, resultConverter: (entry: Entry) => T): Promise<T | undefined> {
    const entry = await this.execute<Entry>("read", { key });
    if (typeof entry === "object") {
      return resultConverter(entry);
    }
    if (entry === 404) {
      return undefined;
    }
    throw new Error("unexpected response: " + entry);
  }

  protected fetchBackendIndexPage(): Promise<BackendIndexPage | undefined> {
    return this.fetchPage(INDEX_PAGE_KEY, (entry) => ({
      data: toByteArray(entry.data),
      transactionId: parseInt(entry.version),
    }));
  }

  protected fetchBackendPage(identifier: BackendPageIdentifier): Promise<BackendPage | undefined> {
    return this.fetchPage(toKey(identifier), (entry) => ({
      identifier,
      data: toByteArray(entry.data),
    }));
  }

  private convertWriteResponse(response: unknown): boolean {
    if (response === 204 || response === 409) {
      return response === 204;
    }
    throw new Error("unexpected response: " + response);
  }

  protected async writeBackendPage(page: BackendPage): Promise<boolean> {
    const response = await this.execute("create", {
      key: toKey(page.identifier),
      version: "0",
      data: fromByteArray(page.data),
    });
    return this.convertWriteResponse(response);
  }

  protected async writeIndexPage(indexPage: BackendIndexPage, previousTransactionId: number): Promise<boolean> {
    const operation = previousTransactionId > 0 ? "update" : "create";
    const response = await this.execute(operation, {
      key: INDEX_PAGE_KEY,
      expectedVersion: operation === "update" ? previousTransactionId.toString() : undefined,
      version: indexPage.transactionId.toString(),
      data: fromByteArray(indexPage.data),
    });
    return this.convertWriteResponse(response);
  }

  protected async cleanupObsoleteBackendPages(writtenPages: BackendPage[]): Promise<void> {
    void Promise.all(
      writtenPages.map(async ({ identifier }) => {
        const response = await this.execute<ListKeysResponse>("listKeys", {
          from: toKey({ ...identifier, transactionId: 0 }),
          to: toKey({ ...identifier, transactionId: identifier.transactionId - 1 }),
        });
        if (typeof response === "object") {
          for (const key of response.keys) {
            // do not await/check the result
            void this.execute("delete", {
              key,
              expectedVersion: "0",
            });
          }
        }
      }),
    );
  }
}
