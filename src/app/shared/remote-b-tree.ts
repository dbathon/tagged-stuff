
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
  children?: string[];
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

  private assertProperNode(node: NodeData) {
    this.assert(node.keys.length === node.values.length);
    this.assert(!node.children || node.children.length === node.keys.length + 1);
    // note: do not assert the max (or min) conditions here, existing trees that were created with a different order should just work...
  }

  private assertProperSiblings(node1: NodeData, node2: NodeData) {
    if ((node1.children && !node2.children) || (!node1.children && node2.children)) {
      throw new Error("nodes are not proper siblings");
    }
  }

  private fetchNodeWithCheck(nodeId: string): BTreeNode {
    const result = this.fetchNode(nodeId);
    this.assertProperNode(result);
    return result;
  }

  private newNode(modifications: Modifications, keys: string[], values: string[], children?: string[]): BTreeNode {
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

  getValue(key: string, rootId: string): string | undefined {
    const node = this.fetchNodeWithCheck(rootId);
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

  private scanInternal(parameters: BTreeScanParameters, nodeId: string, result: BTreeEntry[]) {
    const node = this.fetchNodeWithCheck(nodeId);
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
        this.scanInternal(parameters, node.children[index], result);
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
      this.scanInternal(parameters, node.children[index], result);
    }
  }

  scan(parameters: BTreeScanParameters, rootId: string): BTreeEntry[] {
    const result: BTreeEntry[] = [];
    this.scanInternal(parameters, rootId, result);
    return result;
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
    const node = this.fetchNodeWithCheck(nodeId);
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

  private copyAndDelete(strings: string[], deleteIndex: number): string[] {
    this.assert(deleteIndex >= 0 && deleteIndex < strings.length);
    return [...strings.slice(0, deleteIndex), ...strings.slice(deleteIndex + 1)];
  }

  private deleteKeyInternal(key: string | undefined, nodeId: string, modifications: Modifications): DeleteResult | undefined {
    const node = this.fetchNodeWithCheck(nodeId);
    if (node.keys.length <= 0) {
      // node is empty, nothing to do
      return undefined;
    }

    // if key is undefined, then we want to delete the largest key in this tree
    const { index, isKey } = key !== undefined ?
      this.searchKeyOrChildIndex(node, key) :
      {
        index: node.children ? node.children.length - 1 : node.keys.length - 1,
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
      const deleteResult = this.deleteKeyInternal(!isKey ? key : undefined, node.children[index], modifications);

      if (deleteResult !== undefined) {
        const newChildData = deleteResult.newChildData;
        let deletedEntry = deleteResult.deletedEntry!;

        // this node will be replaced/deleted in any case
        modifications.obsoleteNodes.push(node);

        let newKeys = [...node.keys];
        let newValues = [...node.values];
        let newChildren = [...node.children];

        if (isKey) {
          newKeys[index] = deletedEntry.key;
          newValues[index] = deletedEntry.value;

          // replace deletedEntry with the "real" one
          deletedEntry = new BTreeEntry(node.keys[index], node.values[index]);
        }

        if (newChildData.keys.length >= this.minKeys) {
          // just create the new node
          const newNode = this.newNodeFromNodeData(modifications, newChildData);
          newChildren[index] = newNode.id;
        }
        else {
          // borrow or merge
          let borrowMergeDone = false;
          // TODO: handle children in child nodes...
          let leftSibling: BTreeNode | undefined = undefined;
          if (index > 0) {
            // check if we can borrow from left sibling
            leftSibling = this.fetchNodeWithCheck(node.children[index - 1]);
            this.assertProperSiblings(leftSibling, newChildData);
            if (leftSibling.keys.length > this.minKeys) {
              const newLeftSiblingKeyCount = leftSibling.keys.length - 1;
              const newChildNode = this.newNode(modifications,
                [newKeys[index - 1], ...newChildData.keys],
                [newValues[index - 1], ...newChildData.values],
                leftSibling.children && newChildData.children && [leftSibling.children[newLeftSiblingKeyCount + 1], ...newChildData.children]
              );
              newChildren[index] = newChildNode.id;

              const newLeftSibling = this.newNode(modifications,
                leftSibling.keys.slice(0, newLeftSiblingKeyCount),
                leftSibling.values.slice(0, newLeftSiblingKeyCount),
                leftSibling.children && leftSibling.children.slice(0, newLeftSiblingKeyCount + 1)
              );
              newChildren[index - 1] = newLeftSibling.id;
              modifications.obsoleteNodes.push(leftSibling);

              newKeys[index - 1] = leftSibling.keys[newLeftSiblingKeyCount];
              newValues[index - 1] = leftSibling.values[newLeftSiblingKeyCount];

              borrowMergeDone = true;
            }
          }

          let rightSibling: BTreeNode | undefined = undefined;
          if (!borrowMergeDone && index < node.children.length - 1) {
            // check if we can borrow from right sibling
            rightSibling = this.fetchNodeWithCheck(node.children[index + 1]);
            this.assertProperSiblings(newChildData, rightSibling);
            if (rightSibling.keys.length > this.minKeys) {
              const newChildNode = this.newNode(modifications,
                [...newChildData.keys, newKeys[index]],
                [...newChildData.values, newValues[index]],
                newChildData.children && rightSibling.children && [...newChildData.children, rightSibling.children[0]]
              );
              newChildren[index] = newChildNode.id;

              const newRightSibling = this.newNode(modifications,
                rightSibling.keys.slice(1),
                rightSibling.values.slice(1),
                rightSibling.children && rightSibling.children.slice(1)
              );
              newChildren[index + 1] = newRightSibling.id;
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
                leftSibling.children && newChildData.children && [...leftSibling.children, ...newChildData.children]
              );

              modifications.obsoleteNodes.push(leftSibling);
            }
            else if (rightSibling !== undefined) {
              deleteIndex = index;
              newMergedChildNode = this.newNode(modifications,
                [...newChildData.keys, newKeys[deleteIndex], ...rightSibling.keys],
                [...newChildData.values, newValues[deleteIndex], ...rightSibling.values],
                newChildData.children && rightSibling.children && [...newChildData.children, ...rightSibling.children]
              );

              modifications.obsoleteNodes.push(rightSibling);
            }
            else {
              // cannot happen
              throw new Error("merge not possible");
            }

            newKeys = this.copyAndDelete(newKeys, deleteIndex);
            newValues = this.copyAndDelete(newValues, deleteIndex);
            newChildren = this.copyAndDelete(newChildren, deleteIndex);
            newChildren[deleteIndex] = newMergedChildNode.id;
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

  deleteKey(key: string, rootId: string): BTreeModificationResult {
    this.assertString(key);
    const modifications = new Modifications();
    let newRootId = rootId;

    const delteResult = this.deleteKeyInternal(key, rootId, modifications);
    if (delteResult !== undefined) {
      this.assert(delteResult.deletedEntry.key === key);

      if (delteResult.newChildData.keys.length > 0 || !delteResult.newChildData.children) {
        const newRoot = this.newNodeFromNodeData(modifications, delteResult.newChildData);
        newRootId = newRoot.id;
      }
      else {
        // the tree depth is reduced by one
        this.assert(delteResult.newChildData.children.length === 1);
        newRootId = delteResult.newChildData.children[0];
      }
    }

    return new BTreeModificationResult(newRootId, modifications.newNodes, modifications.obsoleteNodes);
  }

}
