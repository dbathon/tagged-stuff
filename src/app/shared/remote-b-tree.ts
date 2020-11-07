
/**
 * A node of the B-tree. The nodes are immutable: instead of mutating the nodes a modified copy with a new id is created.
 */
export interface BTreeNode {
  readonly id: string;

  /**
   * The ordered keys.
   */
  readonly keys: string[];

  /**
   * The values belonging to the keys, must have the same length as keys.
   */
  readonly values: string[];

  /**
   * The child node ids, if undefined, then this node is a leaf.
   */
  readonly children?: string[];
}

export class BTreeModificationResult {
  constructor(readonly newRootId: string, readonly newNodes: BTreeNode[], readonly obsoleteNodes: BTreeNode[]) { }
}

/**
 * Allows accessing a B-tree whose nodes can be accessed via the fetchNode function.
 *
 * The nodes are immutable, to modify the tree new nodes will be created instead of modifying existing ones.
 * That means that any modification will create (at least) a new root node.
 *
 * The instance of this class has no mutable state, so all methods have a rootId parameter that points to the
 * current root node. If the tree needs to be modified in a method then a BTreeModificationResult is returned
 * and the caller is responsible to apply those changes to the underlying storage that is accessed via
 * fetchNode.
 */
export class RemoteBTree {

  readonly order: number;
  readonly minChildren: number;

  get maxChildren(): number {
    return this.order;
  }

  get maxKeys(): number {
    return this.maxChildren - 1;
  }

  get minKeys(): number {
    return this.minChildren - 1;
  }

  /**
   * @param order
   * @param fetchNode the function used to get the nodes, TODO: change this to be an Observable
   * @param generateId used to generate ids for new nodes, needs to return ids that are not used yet
   */
  constructor(order: number, readonly fetchNode: (nodeId: string) => BTreeNode, readonly generateId: () => string) {
    this.order = Math.ceil(order);
    this.minChildren = Math.ceil(this.order / 2);
    if (this.minKeys < 1 || this.maxKeys < 2) new Error("order is too low: " + order);
  }

  private assert(condition: boolean) {
    if (!condition) {
      throw new Error("assertion failed");
    }
  }

  initializeNewTree(): BTreeModificationResult {
    const newId = this.generateId();
    const newRoot: BTreeNode = {
      id: newId,
      keys: [],
      values: []
    };
    return new BTreeModificationResult(newId, [newRoot], []);
  }

  private searchKeyOrChildIndex(node: BTreeNode, key: string): number {
    let left = 0;
    let right = node.keys.length - 1;
    if (right < 0) {
      throw new Error("node is empty");
    }
    // handle key after last key case
    if (key > node.keys[right]) {
      return right + 1;
    }
    while (right >= left) {
      const current = Math.floor((left + right) / 2);
      const currentKey = node.keys[current];
      if (currentKey == key) {
        // found a key index
        return current;
      }
      if (currentKey > key) {
        if (current == 0 || node.keys[current - 1] < key) {
          // found a child index
          return current;
        }
        right = current - 1;
      }
      else {
        this.assert(currentKey < key);
        left = current + 1;
      }
    }

    throw new Error("searchKeyOrChildIndex did not find an index: " + key + ", " + node.id);
  }

  getValue(key: string, rootId: string): string | undefined {
    const node = this.fetchNode(rootId);
    if (node.keys.length <= 0) {
      // empty root node
      return undefined;
    }
    const index = this.searchKeyOrChildIndex(node, key);
    if (index < node.keys.length && node.keys[index] == key) {
      return node.values[index];
    }
    else if (node.children) {
      return this.getValue(key, node.children[index]);
    }

    return undefined;
  }

}
