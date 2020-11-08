
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

class Modifications {
  readonly newNodes: BTreeNode[] = [];
  readonly obsoleteNodes: BTreeNode[] = [];
}

interface Insert {
  key: string;
  value: string;
  leftChildId?: string;
  rightChildId?: string;
}

interface InsertResult {
  newNodeId?: string;
  splitInsert?: Insert;
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
    if (this.minKeys < 1 || this.maxKeys < 2) {
      throw new Error("order is too low: " + order);
    }
  }

  private assert(condition: boolean) {
    if (!condition) {
      throw new Error("assertion failed");
    }
  }

  private assertString(string?: string): string {
    if (typeof string === 'string') {
      return string;
    }
    throw new Error("not a string: " + string);
  }

  private newNode(modifications: Modifications, keys: string[], values: string[], children?: string[]): BTreeNode {
    this.assert(keys.length === values.length);
    this.assert(!children || children.length === keys.length + 1);
    // note: do not assert the max (or min) conditions here, existing trees that were created with a different order should just work...

    const newId = this.generateId();
    const newNode: BTreeNode = {
      id: newId,
      keys,
      values,
      children
    };
    modifications.newNodes.push(newNode);
    return newNode;
  }

  initializeNewTree(): BTreeModificationResult {
    const modifications = new Modifications();
    const newRoot = this.newNode(modifications, [], []);
    return new BTreeModificationResult(newRoot.id, modifications.newNodes, modifications.obsoleteNodes);
  }

  private searchKeyOrChildIndex(node: BTreeNode, key: string): { index: number, isKey: boolean; } {
    let left = 0;
    let right = node.keys.length - 1;
    if (right < 0) {
      throw new Error("node is empty");
    }
    // handle key after last key case
    if (key > node.keys[right]) {
      return { index: right + 1, isKey: false };
    }
    while (right >= left) {
      const current = Math.floor((left + right) / 2);
      const currentKey = node.keys[current];
      if (currentKey == key) {
        // found a key index
        return { index: current, isKey: true };
      }
      if (currentKey > key) {
        if (current == 0 || node.keys[current - 1] < key) {
          // found a child index
          return { index: current, isKey: false };
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
    const { index, isKey } = this.searchKeyOrChildIndex(node, key);
    if (isKey) {
      return node.values[index];
    }
    else if (node.children) {
      return this.getValue(key, node.children[index]);
    }

    return undefined;
  }

  private copyAndInsert(strings: string[], insertIndex: number, valueToInsert: string): string[] {
    this.assert(insertIndex >= 0 && insertIndex <= strings.length);
    return [...strings.slice(0, insertIndex), this.assertString(valueToInsert), ...strings.slice(insertIndex)];
  }

  private insertIntoNode(node: BTreeNode, index: number, insert: Insert, modifications: Modifications): InsertResult {
    const newKeys = this.copyAndInsert(node.keys, index, insert.key);
    const newValues = this.copyAndInsert(node.values, index, insert.value);
    let newChildren = undefined;
    if (node.children) {
      // first set the right child at index
      const childrenCopy = [...node.children];
      childrenCopy[index] = this.assertString(insert.rightChildId);
      // then insert the left child at index
      newChildren = this.copyAndInsert(childrenCopy, index, this.assertString(insert.leftChildId));
    }
    else {
      this.assert(insert.leftChildId === undefined && insert.rightChildId === undefined);
    }

    modifications.obsoleteNodes.push(node);

    if (newKeys.length <= this.maxKeys) {
      // insert into this node
      const newNode = this.newNode(modifications, newKeys, newValues, newChildren);
      return { newNodeId: newNode.id };
    }
    else {
      // we need to split this node
      const leftSize = this.minKeys;
      const newNodeLeft = this.newNode(modifications, newKeys.slice(0, leftSize), newValues.slice(0, leftSize),
        newChildren && newChildren.slice(0, leftSize + 1));
      const newNodeRight = this.newNode(modifications, newKeys.slice(leftSize + 1), newValues.slice(leftSize + 1),
        newChildren && newChildren.slice(leftSize + 1));
      return {
        splitInsert: {
          key: newKeys[leftSize],
          value: newValues[leftSize],
          leftChildId: newNodeLeft.id,
          rightChildId: newNodeRight.id
        }
      };
    }
  }

  private setValueInternal(key: string, value: string, nodeId: string, modifications: Modifications): InsertResult {
    const node = this.fetchNode(nodeId);
    if (node.keys.length <= 0) {
      // empty root node, just insert
      const newNode = this.newNode(modifications, [key], [value]);
      modifications.obsoleteNodes.push(node);
      return { newNodeId: newNode.id };
    }

    const { index, isKey } = this.searchKeyOrChildIndex(node, key);

    if (isKey) {
      if (node.values[index] == value) {
        // no change required
        return {};
      }
      else {
        // update this node
        const newValues = [...node.values];
        newValues[index] = value;
        const newNode = this.newNode(modifications, [...node.keys], newValues, node.children && [...node.children]);
        modifications.obsoleteNodes.push(node);
        return { newNodeId: newNode.id };
      }
    }
    else if (!node.children) {
      // this node is a leaf node, just insert
      return this.insertIntoNode(node, index, { key, value }, modifications);
    }
    else {
      // recursively set the value in the child
      const insertResult = this.setValueInternal(key, value, node.children[index], modifications);
      if (insertResult.newNodeId !== undefined) {
        // update this node with the new child id
        const newChildren = [...node.children];
        newChildren[index] = insertResult.newNodeId;
        const newNode = this.newNode(modifications, [...node.keys], [...node.values], newChildren);
        modifications.obsoleteNodes.push(node);
        return { newNodeId: newNode.id };
      }
      else if (insertResult.splitInsert) {
        // perform the split insert
        return this.insertIntoNode(node, index, insertResult.splitInsert, modifications);
      }
      else {
        // no changes, reuse the insertResult
        return insertResult;
      }
    }
  }

  setValue(key: string, value: string, rootId: string): BTreeModificationResult {
    this.assertString(key);
    this.assertString(value);
    const modifications = new Modifications();
    let newRootId = rootId;

    const insertResult = this.setValueInternal(key, value, rootId, modifications);
    if (insertResult.newNodeId !== undefined) {
      newRootId = insertResult.newNodeId;
    }
    else if (insertResult.splitInsert) {
      // create new root node
      const insert = insertResult.splitInsert;
      const newRootNode = this.newNode(modifications, [insert.key], [insert.value],
        [this.assertString(insert.leftChildId), this.assertString(insert.rightChildId)]);
      newRootId = newRootNode.id;
    }

    return new BTreeModificationResult(newRootId, modifications.newNodes, modifications.obsoleteNodes);
  }

}
