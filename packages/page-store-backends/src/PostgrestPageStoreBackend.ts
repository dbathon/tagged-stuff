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
    page_number int8,          -- -1 for the index page
    transaction_id int8,       -- always 0 for the index page
    index_transaction_id int8, -- the transaction_id for the index page
    data TEXT not null,
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
    transactionId: number,
  ): Promise<{ data: Uint8Array; indexPageTransactionId?: number } | undefined> {
    const { error, data } = await this.postgrest
      .from(TABLE)
      .select("data, index_transaction_id")
      .eq("store_name", this.storeName)
      .eq("page_number", pageNumber)
      .eq("transaction_id", transactionId);
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
    const { data: dataString, index_transaction_id: indexPageTransactionId } = data[0];
    if (typeof dataString !== "string") {
      throw new Error("got unexpected data: " + JSON.stringify(data[0]));
    }
    return {
      data: toByteArray(dataString),
      indexPageTransactionId: typeof indexPageTransactionId === "number" ? indexPageTransactionId : undefined,
    };
  }

  private async fetchBackendIndexPage(): Promise<BackendIndexPage> {
    const result = await this.fetchPageData(INDEX_PAGE_PAGE_NUMBER, 0);
    if (result) {
      if (result.indexPageTransactionId === undefined) {
        throw new Error("index_transaction_id is missing");
      }
      return {
        data: result.data,
        transactionId: result.indexPageTransactionId,
      };
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
          index_transaction_id: indexPage.transactionId,
          data: fromByteArray(indexPage.data),
        })
        .eq("store_name", this.storeName)
        .eq("page_number", INDEX_PAGE_PAGE_NUMBER)
        .eq("transaction_id", 0)
        .eq("index_transaction_id", previousTransactionId)
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
      const { error } = await this.postgrest.from(TABLE).insert({
        store_name: this.storeName,
        page_number: INDEX_PAGE_PAGE_NUMBER,
        transaction_id: 0,
        index_transaction_id: indexPage.transactionId,
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
