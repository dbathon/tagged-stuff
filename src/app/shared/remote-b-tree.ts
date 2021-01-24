
export interface BTreeNodeChildren {
  /**
   * The child node ids.
   */
  readonly ids: string[];

  /**
   * The sizes of the child trees.
   */
  readonly sizes: number[];
}

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
   * The children, if undefined, then this node is a leaf.
   */
  readonly children?: BTreeNodeChildren;
}

export class BTreeModificationResult {
  constructor(readonly newRootId: string, readonly newNodes: BTreeNode[], readonly obsoleteNodes: BTreeNode[]) { }
}

class Modifications {
  readonly newNodes: BTreeNode[] = [];
  readonly obsoleteNodes: BTreeNode[] = [];
}

interface InsertChild {
  id: string;
  size: number;
}

interface Insert {
  key: string;
  value: string;
  leftChild?: InsertChild;
  rightChild?: InsertChild;
}

interface InsertResult {
  newNode?: InsertChild;
  splitInsert?: Insert;
}

export class BTreeScanParameters {
  constructor(readonly maxResults?: number,
    readonly minKey?: string, readonly maxKey?: string,
    readonly minExclusive: boolean = false, readonly maxExclusive: boolean = true
  ) { }

  includesKey(key: string): boolean {
    if (this.minKey !== undefined) {
      if (this.minExclusive && !(key > this.minKey)) {
        return false;
      }
      if (!this.minExclusive && !(key >= this.minKey)) {
        return false;
      }
    }
    if (this.maxKey !== undefined) {
      if (this.maxExclusive && !(key < this.maxKey)) {
        return false;
      }
      if (!this.maxExclusive && !(key <= this.maxKey)) {
        return false;
      }
    }
    return true;
  }

  maxResultsReached(resultsCount: number): boolean {
    return this.maxResults !== undefined && resultsCount >= this.maxResults;
  }
}

export class BTreeEntry {
  constructor(readonly key: string, readonly value: string) { }
}

interface NodeData {
  keys: string[];
  values: string[];
  children?: BTreeNodeChildren;
}

interface DeleteResult {
  // new child node to be created, but might be too small...
  newChildData: NodeData;
  deletedEntry: BTreeEntry;
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
  constructor(order: number, readonly fetchNode: (nodeId: string) => Promise<BTreeNode>, readonly generateId: () => string) {
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

  private assertNumber(number?: number): number {
    if (typeof number === 'number') {
      return number;
    }
    throw new Error("not a number: " + number);
  }

  private assertProperNode(node: NodeData) {
    this.assert(node.keys.length === node.values.length);
    this.assert(!node.children || (
      node.children.ids.length === node.keys.length + 1 &&
      node.children.sizes.length === node.keys.length + 1
    ));
    // note: do not assert the max (or min) conditions here:
    // existing trees that were created with a different order should just work...
    // and the root node is also allowed to violate those conditions
  }

  private assertProperSiblings(node1: NodeData, node2: NodeData) {
    if ((node1.children && !node2.children) || (!node1.children && node2.children)) {
      throw new Error("nodes are not proper siblings");
    }
  }

  private async fetchNodeWithCheck(nodeId: string): Promise<BTreeNode> {
    const result = await this.fetchNode(nodeId);
    this.assertProperNode(result);
    return result;
  }

  private newNode(modifications: Modifications, keys: string[], values: string[], children?: BTreeNodeChildren): BTreeNode {
    const newId = this.generateId();
    const newNode: BTreeNode = {
      id: newId,
      keys,
      values,
      children
    };
    this.assertProperNode(newNode);
    modifications.newNodes.push(newNode);
    return newNode;
  }

  private newNodeFromNodeData(modifications: Modifications, nodeData: NodeData): BTreeNode {
    return this.newNode(modifications, nodeData.keys, nodeData.values, nodeData.children);
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

  async getValue(key: string, rootId: string): Promise<string | undefined> {
    const node = await this.fetchNodeWithCheck(rootId);
    if (node.keys.length <= 0) {
      // empty root node
      return undefined;
    }
    const { index, isKey } = this.searchKeyOrChildIndex(node, key);
    if (isKey) {
      return node.values[index];
    }
    else if (node.children) {
      return this.getValue(key, node.children.ids[index]);
    }

    return undefined;
  }

  private async scanInternal(parameters: BTreeScanParameters, nodeId: string, result: BTreeEntry[]) {
    const node = await this.fetchNodeWithCheck(nodeId);
    if (node.keys.length <= 0) {
      // empty root node
      return;
    }
    let index = 0;
    if (parameters.minKey !== undefined && node.keys[0] < parameters.minKey) {
      // use search to find the start index
      index = this.searchKeyOrChildIndex(node, parameters.minKey).index;
    }
    // TODO: maybe some child scans could be skipped, e.g. if some key matches the minKey...
    while (index < node.keys.length && !parameters.maxResultsReached(result.length)) {
      if (node.children) {
        await this.scanInternal(parameters, node.children.ids[index], result);
      }
      const key = node.keys[index];
      if (!parameters.includesKey(key)) {
        // we are done
        this.assert(parameters.maxKey !== undefined && key > parameters.maxKey);
        return;
      }
      if (!parameters.maxResultsReached(result.length)) {
        result.push(new BTreeEntry(key, node.values[index]));
      }
      ++index;
    }
    // scan the last child
    if (node.children && !parameters.maxResultsReached(result.length)) {
      await this.scanInternal(parameters, node.children.ids[index], result);
    }
  }

  async scan(parameters: BTreeScanParameters, rootId: string): Promise<BTreeEntry[]> {
    const result: BTreeEntry[] = [];
    await this.scanInternal(parameters, rootId, result);
    return result;
  }

  private copyAndInsert<T>(values: T[], insertIndex: number, valueToInsert: T): T[] {
    this.assert(insertIndex >= 0 && insertIndex <= values.length && valueToInsert !== undefined);
    return [...values.slice(0, insertIndex), valueToInsert, ...values.slice(insertIndex)];
  }

  private sliceChildren(children: BTreeNodeChildren, start: number, end?: number): BTreeNodeChildren {
    return {
      ids: children.ids.slice(start, end),
      sizes: children.sizes.slice(start, end)
    };
  }

  private nodeSize(node: NodeData): number {
    let result = node.keys.length;
    if (node.children) {
      for (const childSize of node.children.sizes) {
        result += childSize;
      }
    }
    return result;
  }

  async getSize(rootId: string): Promise<number> {
    return this.nodeSize(await this.fetchNodeWithCheck(rootId));
  }

  private toInsertChild(node: BTreeNode): InsertChild {
    return {
      id: node.id,
      size: this.nodeSize(node)
    };
  }

  private insertIntoNode(node: BTreeNode, index: number, insert: Insert, modifications: Modifications): InsertResult {
    const newKeys = this.copyAndInsert(node.keys, index, insert.key);
    const newValues = this.copyAndInsert(node.values, index, insert.value);
    let newChildren: BTreeNodeChildren | undefined = undefined;
    if (node.children) {
      newChildren = {
        ids: [...node.children.ids],
        sizes: [...node.children.sizes]
      };
      // first set the right child at index
      newChildren.ids[index] = this.assertString(insert.rightChild?.id);
      newChildren.sizes[index] = this.assertNumber(insert.rightChild?.size);
      // then insert the left child at index
      newChildren = {
        ids: this.copyAndInsert(newChildren.ids, index, this.assertString(insert.leftChild?.id)),
        sizes: this.copyAndInsert(newChildren.sizes, index, this.assertNumber(insert.leftChild?.size))
      };
    }
    else {
      this.assert(insert.leftChild === undefined && insert.rightChild === undefined);
    }

    modifications.obsoleteNodes.push(node);

    if (newKeys.length <= this.maxKeys) {
      // insert into this node
      const newNode = this.newNode(modifications, newKeys, newValues, newChildren);
      return { newNode: this.toInsertChild(newNode) };
    }
    else {
      // we need to split this node
      const leftSize = this.minKeys;
      const newNodeLeft = this.newNode(modifications, newKeys.slice(0, leftSize), newValues.slice(0, leftSize),
        newChildren && this.sliceChildren(newChildren, 0, leftSize + 1));
      const newNodeRight = this.newNode(modifications, newKeys.slice(leftSize + 1), newValues.slice(leftSize + 1),
        newChildren && this.sliceChildren(newChildren, leftSize + 1));
      return {
        splitInsert: {
          key: newKeys[leftSize],
          value: newValues[leftSize],
          leftChild: {
            id: newNodeLeft.id,
            size: this.nodeSize(newNodeLeft)
          },
          rightChild: {
            id: newNodeRight.id,
            size: this.nodeSize(newNodeRight)
          }
        }
      };
    }
  }

  private async setValueInternal(key: string, value: string, nodeId: string, modifications: Modifications): Promise<InsertResult> {
    const node = await this.fetchNodeWithCheck(nodeId);
    if (node.keys.length <= 0) {
      // empty root node, just insert
      const newNode = this.newNode(modifications, [key], [value]);
      modifications.obsoleteNodes.push(node);
      return { newNode: this.toInsertChild(newNode) };
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
        const newNode = this.newNode(modifications, [...node.keys], newValues, node.children && this.sliceChildren(node.children, 0));
        modifications.obsoleteNodes.push(node);
        return { newNode: this.toInsertChild(newNode) };
      }
    }
    else if (!node.children) {
      // this node is a leaf node, just insert
      return this.insertIntoNode(node, index, { key, value }, modifications);
    }
    else {
      // recursively set the value in the child
      const insertResult = await this.setValueInternal(key, value, node.children.ids[index], modifications);
      if (insertResult.newNode !== undefined) {
        // update this node with the new child id
        const newChildren = this.sliceChildren(node.children, 0);
        newChildren.ids[index] = insertResult.newNode.id;
        newChildren.sizes[index] = insertResult.newNode.size;
        const newNode = this.newNode(modifications, [...node.keys], [...node.values], newChildren);
        modifications.obsoleteNodes.push(node);
        return { newNode: this.toInsertChild(newNode) };
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

  async setValue(key: string, value: string, rootId: string): Promise<BTreeModificationResult> {
    this.assertString(key);
    this.assertString(value);
    const modifications = new Modifications();
    let newRootId = rootId;

    const insertResult = await this.setValueInternal(key, value, rootId, modifications);
    if (insertResult.newNode !== undefined) {
      newRootId = insertResult.newNode.id;
    }
    else if (insertResult.splitInsert) {
      // create new root node
      const insert = insertResult.splitInsert;
      const newRootNode = this.newNode(modifications, [insert.key], [insert.value], {
        ids: [this.assertString(insert.leftChild?.id), this.assertString(insert.rightChild?.id)],
        sizes: [this.assertNumber(insert.leftChild?.size), this.assertNumber(insert.rightChild?.size)],
      });
      newRootId = newRootNode.id;
    }

    return new BTreeModificationResult(newRootId, modifications.newNodes, modifications.obsoleteNodes);
  }

  private copyAndDelete<T>(values: T[], deleteIndex: number): T[] {
    this.assert(deleteIndex >= 0 && deleteIndex < values.length);
    return [...values.slice(0, deleteIndex), ...values.slice(deleteIndex + 1)];
  }

  private async deleteKeyInternal(key: string | undefined, nodeId: string, modifications: Modifications): Promise<DeleteResult | undefined> {
    const node = await this.fetchNodeWithCheck(nodeId);
    if (node.keys.length <= 0) {
      // node is empty, nothing to do
      return undefined;
    }

    // if key is undefined, then we want to delete the largest key in this tree
    const { index, isKey } = key !== undefined ?
      this.searchKeyOrChildIndex(node, key) :
      {
        index: node.children ? node.children.ids.length - 1 : node.keys.length - 1,
        isKey: !node.children
      };

    if (!node.children) {
      if (isKey) {
        modifications.obsoleteNodes.push(node);
        return {
          newChildData: {
            keys: this.copyAndDelete(node.keys, index),
            values: this.copyAndDelete(node.values, index)
          },
          deletedEntry: new BTreeEntry(node.keys[index], node.values[index])
        };
      }
      else {
        // key is not in the tree
        return undefined;
      }
    }
    else {
      /**
       * If !isKey then we can just delete the key from the child, otherwise just delete the largest key from the right
       * sub-tree, that entry will then be used to replace the entry in this node, that is supposed to be deleted.
       *
       * TODO: maybe optimize this and potentially also delete from the left sub-tree in the isKey case.
       */
      const deleteResult = await this.deleteKeyInternal(!isKey ? key : undefined, node.children.ids[index], modifications);

      if (deleteResult !== undefined) {
        const newChildData = deleteResult.newChildData;
        let deletedEntry = deleteResult.deletedEntry!;

        // this node will be replaced/deleted in any case
        modifications.obsoleteNodes.push(node);

        let newKeys = [...node.keys];
        let newValues = [...node.values];
        let newChildren = this.sliceChildren(node.children, 0);

        if (isKey) {
          newKeys[index] = deletedEntry.key;
          newValues[index] = deletedEntry.value;

          // replace deletedEntry with the "real" one
          deletedEntry = new BTreeEntry(node.keys[index], node.values[index]);
        }

        if (newChildData.keys.length >= this.minKeys) {
          // just create the new node
          const newNode = this.newNodeFromNodeData(modifications, newChildData);
          newChildren.ids[index] = newNode.id;
          newChildren.sizes[index] = this.nodeSize(newNode);
        }
        else {
          // borrow or merge
          let borrowMergeDone = false;
          // TODO: handle children in child nodes...
          let leftSibling: BTreeNode | undefined = undefined;
          if (index > 0) {
            // check if we can borrow from left sibling
            leftSibling = await this.fetchNodeWithCheck(node.children.ids[index - 1]);
            this.assertProperSiblings(leftSibling, newChildData);
            if (leftSibling.keys.length > this.minKeys) {
              const newLeftSiblingKeyCount = leftSibling.keys.length - 1;
              const newChildNode = this.newNode(modifications,
                [newKeys[index - 1], ...newChildData.keys],
                [newValues[index - 1], ...newChildData.values],
                leftSibling.children && newChildData.children && {
                  ids: [leftSibling.children.ids[newLeftSiblingKeyCount + 1], ...newChildData.children.ids],
                  sizes: [leftSibling.children.sizes[newLeftSiblingKeyCount + 1], ...newChildData.children.sizes]
                }
              );
              newChildren.ids[index] = newChildNode.id;
              newChildren.sizes[index] = this.nodeSize(newChildNode);

              const newLeftSibling = this.newNode(modifications,
                leftSibling.keys.slice(0, newLeftSiblingKeyCount),
                leftSibling.values.slice(0, newLeftSiblingKeyCount),
                leftSibling.children && this.sliceChildren(leftSibling.children, 0, newLeftSiblingKeyCount + 1)
              );
              newChildren.ids[index - 1] = newLeftSibling.id;
              newChildren.sizes[index - 1] = this.nodeSize(newLeftSibling);
              modifications.obsoleteNodes.push(leftSibling);

              newKeys[index - 1] = leftSibling.keys[newLeftSiblingKeyCount];
              newValues[index - 1] = leftSibling.values[newLeftSiblingKeyCount];

              borrowMergeDone = true;
            }
          }

          let rightSibling: BTreeNode | undefined = undefined;
          if (!borrowMergeDone && index < node.children.ids.length - 1) {
            // check if we can borrow from right sibling
            rightSibling = await this.fetchNodeWithCheck(node.children.ids[index + 1]);
            this.assertProperSiblings(newChildData, rightSibling);
            if (rightSibling.keys.length > this.minKeys) {
              const newChildNode = this.newNode(modifications,
                [...newChildData.keys, newKeys[index]],
                [...newChildData.values, newValues[index]],
                newChildData.children && rightSibling.children && {
                  ids: [...newChildData.children.ids, rightSibling.children.ids[0]],
                  sizes: [...newChildData.children.sizes, rightSibling.children.sizes[0]]
                }
              );
              newChildren.ids[index] = newChildNode.id;
              newChildren.sizes[index] = this.nodeSize(newChildNode);

              const newRightSibling = this.newNode(modifications,
                rightSibling.keys.slice(1),
                rightSibling.values.slice(1),
                rightSibling.children && this.sliceChildren(rightSibling.children, 1)
              );
              newChildren.ids[index + 1] = newRightSibling.id;
              newChildren.sizes[index + 1] = this.nodeSize(newRightSibling);
              modifications.obsoleteNodes.push(rightSibling);

              newKeys[index] = rightSibling.keys[0];
              newValues[index] = rightSibling.values[0];

              borrowMergeDone = true;
            }
          }

          if (!borrowMergeDone) {
            // no borrowing possible, merge two child nodes
            let deleteIndex: number | undefined = undefined;
            let newMergedChildNode: BTreeNode | undefined = undefined;
            if (leftSibling !== undefined) {
              deleteIndex = index - 1;
              newMergedChildNode = this.newNode(modifications,
                [...leftSibling.keys, newKeys[deleteIndex], ...newChildData.keys],
                [...leftSibling.values, newValues[deleteIndex], ...newChildData.values],
                leftSibling.children && newChildData.children && {
                  ids: [...leftSibling.children.ids, ...newChildData.children.ids],
                  sizes: [...leftSibling.children.sizes, ...newChildData.children.sizes]
                }
              );

              modifications.obsoleteNodes.push(leftSibling);
            }
            else if (rightSibling !== undefined) {
              deleteIndex = index;
              newMergedChildNode = this.newNode(modifications,
                [...newChildData.keys, newKeys[deleteIndex], ...rightSibling.keys],
                [...newChildData.values, newValues[deleteIndex], ...rightSibling.values],
                newChildData.children && rightSibling.children && {
                  ids: [...newChildData.children.ids, ...rightSibling.children.ids],
                  sizes: [...newChildData.children.sizes, ...rightSibling.children.sizes]
                }
              );

              modifications.obsoleteNodes.push(rightSibling);
            }
            else {
              // cannot happen
              throw new Error("merge not possible");
            }

            newKeys = this.copyAndDelete(newKeys, deleteIndex);
            newValues = this.copyAndDelete(newValues, deleteIndex);
            newChildren = {
              ids: this.copyAndDelete(newChildren.ids, deleteIndex),
              sizes: this.copyAndDelete(newChildren.sizes, deleteIndex)
            };
            newChildren.ids[deleteIndex] = newMergedChildNode.id;
            newChildren.sizes[deleteIndex] = this.nodeSize(newMergedChildNode);
          }
        }
        return {
          newChildData: {
            keys: newKeys,
            values: newValues,
            children: newChildren
          },
          deletedEntry
        };
      }
      else {
        this.assert(!isKey);

        // nothing to do
        return undefined;
      }
    }
  }

  async deleteKey(key: string, rootId: string): Promise<BTreeModificationResult> {
    this.assertString(key);
    const modifications = new Modifications();
    let newRootId = rootId;

    const deleteResult = await this.deleteKeyInternal(key, rootId, modifications);
    if (deleteResult !== undefined) {
      this.assert(deleteResult.deletedEntry.key === key);

      if (deleteResult.newChildData.keys.length > 0 || !deleteResult.newChildData.children) {
        const newRoot = this.newNodeFromNodeData(modifications, deleteResult.newChildData);
        newRootId = newRoot.id;
      }
      else {
        // the tree depth is reduced by one
        this.assert(deleteResult.newChildData.children.ids.length === 1);
        newRootId = deleteResult.newChildData.children.ids[0];
      }
    }

    return new BTreeModificationResult(newRootId, modifications.newNodes, modifications.obsoleteNodes);
  }

}
