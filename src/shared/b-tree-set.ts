import { BTreeModificationResult, BTreeNode, BTreeScanConsumer, RemoteBTree } from "./remote-b-tree";
import { Result, UNDEFINED_RESULT } from "./result";

export class BTreeSet {
  readonly data: Map<string, BTreeNode> = new Map();
  readonly tree: RemoteBTree;
  readonly fetchNode: (id: string) => Result<BTreeNode>;
  rootId: string;
  fetchNodeWithPromise = false;

  constructor(maxNodeSize: number) {
    const fetchNodeRaw = (id: string): BTreeNode => {
      const node = this.data.get(id);
      if (node === undefined) {
        throw new Error("node not found: " + id);
      }
      return node;
    };
    const fetchNodePromise = async (id: string): Promise<BTreeNode> => fetchNodeRaw(id);
    const fetchNode = (id: string): Result<BTreeNode> => {
      if (this.fetchNodeWithPromise) {
        return Result.withPromise(fetchNodePromise(id));
      } else {
        return Result.withValue(fetchNodeRaw(id));
      }
    };
    this.fetchNode = fetchNode;
    let id = 0;
    const generateId = () => {
      ++id;
      return "" + id;
    };
    this.tree = new RemoteBTree(maxNodeSize, fetchNode, generateId, 3);

    // dummy assignment for the type checker
    this.rootId = "";
    this.apply(this.tree.initializeNewTree());
  }

  apply(modificationResult: BTreeModificationResult): boolean {
    this.rootId = modificationResult.newRootId;
    modificationResult.obsoleteNodes.forEach((node) => this.data.delete(node.id));
    modificationResult.newNodes.forEach((node) => this.data.set(node.id, node));
    return modificationResult.obsoleteNodes.length > 0 || modificationResult.newNodes.length > 0;
  }

  contains(key: string): Result<boolean> {
    return this.tree.containsKey(key, this.rootId);
  }

  insert(key: string): Result<boolean> {
    return this.tree.insertKey(key, this.rootId).transform((result) => this.apply(result));
  }

  delete(key: string): Result<boolean> {
    return this.tree.deleteKey(key, this.rootId).transform((result) => this.apply(result));
  }

  scan(minKey: string | undefined, scanConsumer: BTreeScanConsumer): Result<void> {
    return this.tree.scan(minKey, scanConsumer, this.rootId);
  }

  simpleScan(maxResults?: number, minKey?: string): Result<string[]> {
    const result: string[] = [];
    if (maxResults !== undefined && maxResults <= 0) {
      return Result.withValue(result);
    }
    return this.scan(minKey, (key) => {
      result.push(key);
      if (maxResults !== undefined && result.length >= maxResults) {
        return UNDEFINED_RESULT;
      }
      return Result.withValue(key);
    }).transform((_) => result);
  }

  getKeyCount(): Result<number> {
    return this.tree.getKeyCount(this.rootId);
  }

  clear() {
    this.data.clear();
    this.apply(this.tree.initializeNewTree());
  }

  private async dumpTreeInternal(nodeId: string, indent: string = "", keyCountFromParent?: number): Promise<string[]> {
    const node = await this.fetchNode(nodeId).toPromise();
    const jsonLength = JSON.stringify(node).length;
    const keyCountFromParentString = keyCountFromParent !== undefined ? " (" + keyCountFromParent + ")" : "";
    const nodeIdAndSizes = `${indent}${nodeId} (${jsonLength})${keyCountFromParentString}:`;
    if (node.children) {
      let result: string[] = [];
      result.push(nodeIdAndSizes);
      const nextIndent = indent + "    ";
      for (let i = 0; i < node.keys.length; ++i) {
        result.push(...(await this.dumpTreeInternal(node.children.ids[i], nextIndent, node.children.keyCounts[i])));
        result.push(indent + "* " + node.keys[i]);
      }
      result.push(
        ...(await this.dumpTreeInternal(
          node.children.ids[node.keys.length],
          nextIndent,
          node.children.keyCounts[node.keys.length]
        ))
      );
      return result;
    } else {
      return [nodeIdAndSizes + " " + node.keys.join(", ")];
    }
  }

  dumpTree(): Promise<string[]> {
    return this.dumpTreeInternal(this.rootId);
  }
}
