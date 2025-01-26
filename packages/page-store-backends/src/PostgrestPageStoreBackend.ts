import { PostgrestClient } from "@supabase/postgrest-js";
import { fromByteArray, toByteArray } from "base64-js";
import type {
  BackendIndexPage,
  BackendPage,
  BackendPageIdentifier,
  BackendReadResult,
  PageStoreBackend,
} from "page-store";

/*
Expected table:
create table page_store_page (
    store_name text,
    page_number int8,
    transaction_id int8,
    data TEXT,
    PRIMARY KEY (store_name, page_number, transaction_id)
);
*/

const TABLE = "page_store_page";

const INDEX_PAGE_PAGE_NUMBER = -1;

export class PostgrestPageStoreBackend implements PageStoreBackend {
  // Support up to 64KB, this limit is arbitrary, it could be increased...
  readonly maxPageSize = 1 << 16;

  private readonly postgrest: PostgrestClient;

  constructor(
    readonly url: string,
    token: string,
    readonly storeName: string,
  ) {
    this.postgrest = new PostgrestClient(url, {
      headers: {
        Authorization: "Bearer " + token,
        // this is needed when Supabase is used
        apikey: token,
      },
    });
  }

  private async fetchPageData(
    pageNumber: number,
    transactionId?: number,
  ): Promise<{ transactionId: number; data: Uint8Array } | undefined> {
    let query = this.postgrest
      .from(TABLE)
      .select("transaction_id, data")
      .eq("store_name", this.storeName)
      .eq("page_number", pageNumber);
    if (transactionId !== undefined) {
      query = query.eq("transaction_id", transactionId);
    } else {
      query = query.limit(2);
    }
    const { error, data } = await query;
    if (error) {
      throw error;
    }
    if (!data) {
      throw new Error("got no data");
    }
    if (!data.length) {
      return undefined;
    }
    if (data.length > 1) {
      throw new Error("unexpected number of results");
    }
    const { transaction_id: resultTransactionId, data: dataString } = data[0];
    if (typeof resultTransactionId !== "number" || typeof dataString !== "string") {
      throw new Error("got unexpected data: " + JSON.stringify(data[0]));
    }
    return {
      transactionId: resultTransactionId,
      data: toByteArray(dataString),
    };
  }

  private async fetchBackendIndexPage(): Promise<BackendIndexPage> {
    const result = await this.fetchPageData(INDEX_PAGE_PAGE_NUMBER);
    if (result) {
      return result;
    }
    // return initial index page
    return {
      transactionId: 0,
      data: new Uint8Array(0),
    };
  }

  private async fetchBackendPage(identifier: BackendPageIdentifier): Promise<BackendPage | undefined> {
    if (identifier.pageNumber < 0) {
      throw new Error("unexpected page number");
    }
    const result = await this.fetchPageData(identifier.pageNumber, identifier.transactionId);
    return (
      result && {
        identifier,
        data: result.data,
      }
    );
  }

  async readPages(includeIndexPage: boolean, pageIdentifiers: BackendPageIdentifier[]): Promise<BackendReadResult> {
    // fetch everything in parallel
    const indexPagePromise: Promise<BackendIndexPage | undefined> = includeIndexPage
      ? this.fetchBackendIndexPage()
      : Promise.resolve(undefined);
    const pagesPromise: Promise<(BackendPage | undefined)[]> = Promise.all(
      pageIdentifiers.map((identifier) => this.fetchBackendPage(identifier)),
    );

    const [indexPage, pages] = await Promise.all([indexPagePromise, pagesPromise]);

    return {
      indexPage,
      pages: pages.filter((page) => !!page),
    };
  }

  async writePages(indexPage: BackendIndexPage, previousTransactionId: number, pages: BackendPage[]): Promise<boolean> {
    // TODO: batching etc.
    for (const page of pages) {
      const { error } = await this.postgrest.from(TABLE).insert({
        store_name: this.storeName,
        page_number: page.identifier.pageNumber,
        transaction_id: page.identifier.transactionId,
        data: fromByteArray(page.data),
      });
      if (error) {
        return false;
      }
    }

    // all backend pages were inserted successfully, so now try the index page update
    if (previousTransactionId > 0) {
      const { error, data } = await this.postgrest
        .from(TABLE)
        .update({
          transaction_id: indexPage.transactionId,
          data: fromByteArray(indexPage.data),
        })
        .eq("store_name", this.storeName)
        .eq("page_number", INDEX_PAGE_PAGE_NUMBER)
        .eq("transaction_id", previousTransactionId)
        .select("transaction_id");

      if (error || !data) {
        throw error;
      }
      if (!data.length) {
        // the update did not work
        return false;
      }
    } else {
      // special case, the first insert of the index page
      // TODO: how to do this safely..
      const { error } = await this.postgrest.from(TABLE).insert({
        store_name: this.storeName,
        page_number: INDEX_PAGE_PAGE_NUMBER,
        transaction_id: indexPage.transactionId,
        data: fromByteArray(indexPage.data),
      });
      if (error) {
        return false;
      }
    }

    // TODO: cleanup obsolete pages
    return true;
  }
}
