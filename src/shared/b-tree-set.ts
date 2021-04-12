import { BTreeModificationResult, BTreeNode, BTreeScanParameters, RemoteBTree } from "./remote-b-tree";
import { Result } from "./result";

export class BTreeSet {

  readonly data: Map<string, BTreeNode> = new Map();
  readonly tree: RemoteBTree;
  readonly fetchNode: (id: string) => Result<BTreeNode>;
  rootId: string;
  fetchNodeWithPromise = false;

  constructor(treeOrder: number) {
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
      }
      else {
        return Result.withValue(fetchNodeRaw(id));
      }
    };
    this.fetchNode = fetchNode;
    let id = 0;
    const generateId = () => {
      ++id;
      return "" + id;
    };
    this.tree = new RemoteBTree(Math.max(treeOrder, 3), fetchNode, generateId);

    // dummy assignment for the type checker
    this.rootId = "";
    this.apply(this.tree.initializeNewTree());
  }

  apply(modificationResult: BTreeModificationResult): boolean {
    this.rootId = modificationResult.newRootId;
    modificationResult.obsoleteNodes.forEach(node => this.data.delete(node.id));
    modificationResult.newNodes.forEach(node => this.data.set(node.id, node));
    return modificationResult.obsoleteNodes.length > 0 || modificationResult.newNodes.length > 0;
  }

  contains(key: string): Result<boolean> {
    return this.tree.containsKey(key, this.rootId);
  }

  insert(key: string): Result<boolean> {
    return this.tree.insertKey(key, this.rootId).transform(result => this.apply(result));
  }

  delete(key: string): Result<boolean> {
    return this.tree.deleteKey(key, this.rootId).transform(result => this.apply(result));
  }

  scan(parameters?: BTreeScanParameters): Result<string[]> {
    return this.tree.scan(parameters || new BTreeScanParameters(), this.rootId);
  }

  getSize(): Result<number> {
    return this.tree.getSize(this.rootId);
  }

  clear() {
    this.data.clear();
    this.apply(this.tree.initializeNewTree());
  }

  private async dumpTreeInternal(nodeId: string, indent: string = "", sizeFromParent?: number): Promise<string[]> {
    const node = await this.fetchNode(nodeId).toPromise();
    const sizeFromParentString = sizeFromParent !== undefined ? " (" + sizeFromParent + ")" : "";
    if (node.children) {
      let result: string[] = [];
      result.push(indent + nodeId + sizeFromParentString + ":");
      const nextIndent = indent + "    ";
      for (let i = 0; i < node.keys.length; ++i) {
        result.push(...await this.dumpTreeInternal(node.children.ids[i], nextIndent, node.children.sizes[i]));
        result.push(indent + "* " + node.keys[i]);
      }
      result.push(...await this.dumpTreeInternal(node.children.ids[node.keys.length], nextIndent, node.children.sizes[node.keys.length]));
      return result;
    }
    else {
      return [indent + nodeId + sizeFromParentString + ": " + node.keys.join(", ")];
    }
  };

  dumpTree(): Promise<string[]> {
    return this.dumpTreeInternal(this.rootId);
  }

}
