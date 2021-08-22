import { Result, FALSE_RESULT, TRUE_RESULT, UNDEFINED_RESULT } from "./result";

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
   * The children, if undefined, then this node is a leaf.
   */
  readonly children?: BTreeNodeChildren;
}

export class BTreeModificationResult {
  constructor(readonly newRootId: string, readonly newNodes: BTreeNode[], readonly obsoleteNodes: BTreeNode[]) {}
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
  leftChild?: InsertChild;
  rightChild?: InsertChild;
}

interface InsertResult {
  newNode?: InsertChild;
  splitInsert?: Insert;
}

export class BTreeScanParameters {
  constructor(
    readonly maxResults?: number,
    readonly minKey?: string,
    readonly maxKey?: string,
    readonly minExclusive: boolean = false,
    readonly maxExclusive: boolean = true
  ) {}

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

interface NodeData {
  keys: string[];
  children?: BTreeNodeChildren;
}

interface DeleteResult {
  // new child node to be created, but might be too small...
  newChildData: NodeData;
  deletedKey: string;
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
   * @param fetchNode the function used to get the nodes
   * @param generateId used to generate ids for new nodes, needs to return ids that are not used yet
   */
  constructor(
    order: number,
    readonly fetchNode: (nodeId: string) => Result<BTreeNode>,
    readonly generateId: () => string
  ) {
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
    if (typeof string === "string") {
      return string;
    }
    throw new Error("not a string: " + string);
  }

  private assertNumber(number?: number): number {
    if (typeof number === "number") {
      return number;
    }
    throw new Error("not a number: " + number);
  }

  private assertProperNode(node: NodeData) {
    this.assert(
      !node.children ||
        (node.children.ids.length === node.keys.length + 1 && node.children.sizes.length === node.keys.length + 1)
    );
    // note: do not assert the max (or min) conditions here:
    // existing trees that were created with a different order should just work...
    // and the root node is also allowed to violate those conditions
  }

  private assertProperSiblings(node1: NodeData, node2: NodeData) {
    if ((node1.children && !node2.children) || (!node1.children && node2.children)) {
      throw new Error("nodes are not proper siblings");
    }
  }

  private fetchNodeWithCheck(nodeId: string): Result<BTreeNode> {
    if (this.fetchNode === undefined) {
      throw new Error();
    }
    return this.fetchNode(nodeId).transform((node) => {
      this.assertProperNode(node);
      return node;
    });
  }

  private newNode(modifications: Modifications, keys: string[], children?: BTreeNodeChildren): BTreeNode {
    const newId = this.generateId();
    const newNode: BTreeNode = {
      id: newId,
      keys,
      children,
    };
    this.assertProperNode(newNode);
    modifications.newNodes.push(newNode);
    return newNode;
  }

  private newNodeFromNodeData(modifications: Modifications, nodeData: NodeData): BTreeNode {
    return this.newNode(modifications, nodeData.keys, nodeData.children);
  }

  initializeNewTree(): BTreeModificationResult {
    const modifications = new Modifications();
    const newRoot = this.newNode(modifications, []);
    return new BTreeModificationResult(newRoot.id, modifications.newNodes, modifications.obsoleteNodes);
  }

  private searchKeyOrChildIndex(node: BTreeNode, key: string): { index: number; isKey: boolean } {
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
      } else {
        this.assert(currentKey < key);
        left = current + 1;
      }
    }

    throw new Error("searchKeyOrChildIndex did not find an index: " + key + ", " + node.id);
  }

  containsKey(key: string, rootId: string): Result<boolean> {
    return this.fetchNodeWithCheck(rootId).transform((node) => {
      if (node.keys.length <= 0) {
        // empty root node
        return FALSE_RESULT;
      }
      const { index, isKey } = this.searchKeyOrChildIndex(node, key);
      if (isKey) {
        return TRUE_RESULT;
      } else if (node.children) {
        return this.containsKey(key, node.children.ids[index]);
      }

      return FALSE_RESULT;
    });
  }

  private scanInternal(parameters: BTreeScanParameters, nodeId: string, result: string[]): Result<void> {
    return this.fetchNodeWithCheck(nodeId).transform((node) => {
      if (node.keys.length <= 0) {
        // empty root node
        return UNDEFINED_RESULT;
      }
      let startIndex = 0;
      if (parameters.minKey !== undefined && node.keys[0] < parameters.minKey) {
        // use search to find the start index
        startIndex = this.searchKeyOrChildIndex(node, parameters.minKey).index;
      }

      const step = (index: number): Result<void> => {
        let childScanResult: Result<void>;
        if (node.children && index < node.children.ids.length && !parameters.maxResultsReached(result.length)) {
          childScanResult = this.scanInternal(parameters, node.children.ids[index], result);
        } else {
          childScanResult = UNDEFINED_RESULT;
        }

        const doneResult: Result<boolean> = childScanResult.transform((_) => {
          if (index < node.keys.length && !parameters.maxResultsReached(result.length)) {
            const key = node.keys[index];
            if (!parameters.includesKey(key)) {
              // we are done
              this.assert(parameters.maxKey !== undefined && key > parameters.maxKey);
              return TRUE_RESULT;
            } else {
              result.push(key);
              return FALSE_RESULT;
            }
          } else {
            return TRUE_RESULT;
          }
        });

        return doneResult.transform((done) => {
          if (done) {
            return UNDEFINED_RESULT;
          } else {
            return step(index + 1);
          }
        });
      };

      return step(startIndex);
    });
  }

  scan(parameters: BTreeScanParameters, rootId: string): Result<string[]> {
    const result: string[] = [];
    return this.scanInternal(parameters, rootId, result).transform((_) => result);
  }

  private copyAndInsert<T>(values: T[], insertIndex: number, valueToInsert: T): T[] {
    this.assert(insertIndex >= 0 && insertIndex <= values.length && valueToInsert !== undefined);
    return [...values.slice(0, insertIndex), valueToInsert, ...values.slice(insertIndex)];
  }

  private sliceChildren(children: BTreeNodeChildren, start: number, end?: number): BTreeNodeChildren {
    return {
      ids: children.ids.slice(start, end),
      sizes: children.sizes.slice(start, end),
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

  getSize(rootId: string): Result<number> {
    return this.fetchNodeWithCheck(rootId).transform((node) => this.nodeSize(node));
  }

  private toInsertChild(node: BTreeNode): InsertChild {
    return {
      id: node.id,
      size: this.nodeSize(node),
    };
  }

  private insertIntoNode(node: BTreeNode, index: number, insert: Insert, modifications: Modifications): InsertResult {
    const newKeys = this.copyAndInsert(node.keys, index, insert.key);
    let newChildren: BTreeNodeChildren | undefined = undefined;
    if (node.children) {
      newChildren = {
        ids: [...node.children.ids],
        sizes: [...node.children.sizes],
      };
      // first set the right child at index
      newChildren.ids[index] = this.assertString(insert.rightChild?.id);
      newChildren.sizes[index] = this.assertNumber(insert.rightChild?.size);
      // then insert the left child at index
      newChildren = {
        ids: this.copyAndInsert(newChildren.ids, index, this.assertString(insert.leftChild?.id)),
        sizes: this.copyAndInsert(newChildren.sizes, index, this.assertNumber(insert.leftChild?.size)),
      };
    } else {
      this.assert(insert.leftChild === undefined && insert.rightChild === undefined);
    }

    modifications.obsoleteNodes.push(node);

    if (newKeys.length <= this.maxKeys) {
      // insert into this node
      const newNode = this.newNode(modifications, newKeys, newChildren);
      return { newNode: this.toInsertChild(newNode) };
    } else {
      // we need to split this node
      const leftSize = this.minKeys;
      const newNodeLeft = this.newNode(
        modifications,
        newKeys.slice(0, leftSize),
        newChildren && this.sliceChildren(newChildren, 0, leftSize + 1)
      );
      const newNodeRight = this.newNode(
        modifications,
        newKeys.slice(leftSize + 1),
        newChildren && this.sliceChildren(newChildren, leftSize + 1)
      );
      return {
        splitInsert: {
          key: newKeys[leftSize],
          leftChild: {
            id: newNodeLeft.id,
            size: this.nodeSize(newNodeLeft),
          },
          rightChild: {
            id: newNodeRight.id,
            size: this.nodeSize(newNodeRight),
          },
        },
      };
    }
  }

  private insertKeyInternal(key: string, nodeId: string, modifications: Modifications): Result<InsertResult> {
    return this.fetchNodeWithCheck(nodeId).transform((node) => {
      if (node.keys.length <= 0) {
        // empty root node, just insert
        const newNode = this.newNode(modifications, [key]);
        modifications.obsoleteNodes.push(node);
        return { newNode: this.toInsertChild(newNode) };
      }

      const { index, isKey } = this.searchKeyOrChildIndex(node, key);

      if (isKey) {
        // no change required
        return {};
      } else if (!node.children) {
        // this node is a leaf node, just insert
        return this.insertIntoNode(node, index, { key }, modifications);
      } else {
        // recursively insert the key in the child
        return this.insertKeyInternal(key, node.children.ids[index], modifications).transform((insertResult) => {
          if (insertResult.newNode !== undefined) {
            // update this node with the new child id
            const newChildren = this.sliceChildren(node.children!, 0);
            newChildren.ids[index] = insertResult.newNode.id;
            newChildren.sizes[index] = insertResult.newNode.size;
            const newNode = this.newNode(modifications, [...node.keys], newChildren);
            modifications.obsoleteNodes.push(node);
            return { newNode: this.toInsertChild(newNode) };
          } else if (insertResult.splitInsert) {
            // perform the split insert
            return this.insertIntoNode(node, index, insertResult.splitInsert, modifications);
          } else {
            // no changes, reuse the insertResult
            return insertResult;
          }
        });
      }
    });
  }

  insertKey(key: string, rootId: string): Result<BTreeModificationResult> {
    this.assertString(key);
    const modifications = new Modifications();
    let newRootId = rootId;

    return this.insertKeyInternal(key, rootId, modifications).transform((insertResult) => {
      if (insertResult.newNode !== undefined) {
        newRootId = insertResult.newNode.id;
      } else if (insertResult.splitInsert) {
        // create new root node
        const insert = insertResult.splitInsert;
        const newRootNode = this.newNode(modifications, [insert.key], {
          ids: [this.assertString(insert.leftChild?.id), this.assertString(insert.rightChild?.id)],
          sizes: [this.assertNumber(insert.leftChild?.size), this.assertNumber(insert.rightChild?.size)],
        });
        newRootId = newRootNode.id;
      }

      return new BTreeModificationResult(newRootId, modifications.newNodes, modifications.obsoleteNodes);
    });
  }

  private copyAndDelete<T>(values: T[], deleteIndex: number): T[] {
    this.assert(deleteIndex >= 0 && deleteIndex < values.length);
    return [...values.slice(0, deleteIndex), ...values.slice(deleteIndex + 1)];
  }

  private deleteKeyInternal(
    key: string | undefined,
    nodeId: string,
    modifications: Modifications
  ): Result<DeleteResult | undefined> {
    return this.fetchNodeWithCheck(nodeId).transform((node) => {
      if (node.keys.length <= 0) {
        // node is empty, nothing to do
        return undefined;
      }

      // if key is undefined, then we want to delete the largest key in this tree
      const { index, isKey } =
        key !== undefined
          ? this.searchKeyOrChildIndex(node, key)
          : {
              index: node.children ? node.children.ids.length - 1 : node.keys.length - 1,
              isKey: !node.children,
            };

      if (!node.children) {
        if (isKey) {
          modifications.obsoleteNodes.push(node);
          return {
            newChildData: {
              keys: this.copyAndDelete(node.keys, index),
            },
            deletedKey: node.keys[index],
          };
        } else {
          // key is not in the tree
          return undefined;
        }
      } else {
        // capture node.children in a variable that is not undefined to help the type checker
        const nodeChildren: BTreeNodeChildren = node.children;

        /**
         * If !isKey then we can just delete the key from the child, otherwise just delete the largest key from the right
         * sub-tree, that key will then be used to replace the key in this node, that is supposed to be deleted.
         *
         * TODO: maybe optimize this and potentially also delete from the left sub-tree in the isKey case.
         */
        const deleteKeyInternalResult = this.deleteKeyInternal(
          !isKey ? key : undefined,
          node.children.ids[index],
          modifications
        );
        return deleteKeyInternalResult.transform((deleteResult) => {
          if (deleteResult === undefined) {
            this.assert(!isKey);

            // nothing to do
            return undefined;
          } else {
            const newChildData = deleteResult.newChildData;

            // this node will be replaced/deleted in any case
            modifications.obsoleteNodes.push(node);

            const newKeys = [...node.keys];
            const newChildren = this.sliceChildren(nodeChildren, 0);

            if (isKey) {
              newKeys[index] = deleteResult.deletedKey;
            }
            const deletedKey = isKey
              ? // replace deletedKey with the "real" one
                node.keys[index]
              : deleteResult.deletedKey;

            const result: DeleteResult = {
              newChildData: {
                keys: newKeys,
                children: newChildren,
              },
              deletedKey,
            };

            if (newChildData.keys.length >= this.minKeys) {
              // just create the new node
              const newNode = this.newNodeFromNodeData(modifications, newChildData);
              newChildren.ids[index] = newNode.id;
              newChildren.sizes[index] = this.nodeSize(newNode);
              return result;
            } else {
              // borrow or merge
              // TODO: maybe always fetch both siblings (if available) at once (once we have "multi fetch")
              let mergeStateResult: Result<{
                borrowMergeDone: boolean;
                leftSibling?: BTreeNode;
                rightSibling?: BTreeNode;
              }>;

              if (index > 0) {
                // check if we can borrow from left sibling
                mergeStateResult = this.fetchNodeWithCheck(nodeChildren.ids[index - 1]).transform((leftSibling) => {
                  this.assertProperSiblings(leftSibling, newChildData);
                  if (leftSibling.keys.length > this.minKeys) {
                    const newLeftSiblingKeyCount = leftSibling.keys.length - 1;
                    const newChildNode = this.newNode(
                      modifications,
                      [newKeys[index - 1], ...newChildData.keys],
                      leftSibling.children &&
                        newChildData.children && {
                          ids: [leftSibling.children.ids[newLeftSiblingKeyCount + 1], ...newChildData.children.ids],
                          sizes: [
                            leftSibling.children.sizes[newLeftSiblingKeyCount + 1],
                            ...newChildData.children.sizes,
                          ],
                        }
                    );
                    newChildren.ids[index] = newChildNode.id;
                    newChildren.sizes[index] = this.nodeSize(newChildNode);

                    const newLeftSibling = this.newNode(
                      modifications,
                      leftSibling.keys.slice(0, newLeftSiblingKeyCount),
                      leftSibling.children && this.sliceChildren(leftSibling.children, 0, newLeftSiblingKeyCount + 1)
                    );
                    newChildren.ids[index - 1] = newLeftSibling.id;
                    newChildren.sizes[index - 1] = this.nodeSize(newLeftSibling);
                    modifications.obsoleteNodes.push(leftSibling);

                    newKeys[index - 1] = leftSibling.keys[newLeftSiblingKeyCount];

                    return {
                      borrowMergeDone: true,
                      leftSibling,
                    };
                  } else {
                    return {
                      borrowMergeDone: false,
                      leftSibling,
                    };
                  }
                });
              } else {
                mergeStateResult = Result.withValue({
                  borrowMergeDone: false,
                });
              }

              mergeStateResult = mergeStateResult.transform((mergeState) => {
                if (!mergeState.borrowMergeDone && index < nodeChildren.ids.length - 1) {
                  // check if we can borrow from right sibling
                  return this.fetchNodeWithCheck(nodeChildren.ids[index + 1]).transform((rightSibling) => {
                    this.assertProperSiblings(newChildData, rightSibling);
                    if (rightSibling.keys.length > this.minKeys) {
                      const newChildNode = this.newNode(
                        modifications,
                        [...newChildData.keys, newKeys[index]],
                        newChildData.children &&
                          rightSibling.children && {
                            ids: [...newChildData.children.ids, rightSibling.children.ids[0]],
                            sizes: [...newChildData.children.sizes, rightSibling.children.sizes[0]],
                          }
                      );
                      newChildren.ids[index] = newChildNode.id;
                      newChildren.sizes[index] = this.nodeSize(newChildNode);

                      const newRightSibling = this.newNode(
                        modifications,
                        rightSibling.keys.slice(1),
                        rightSibling.children && this.sliceChildren(rightSibling.children, 1)
                      );
                      newChildren.ids[index + 1] = newRightSibling.id;
                      newChildren.sizes[index + 1] = this.nodeSize(newRightSibling);
                      modifications.obsoleteNodes.push(rightSibling);

                      newKeys[index] = rightSibling.keys[0];

                      return {
                        ...mergeState,
                        borrowMergeDone: true,
                        rightSibling,
                      };
                    } else {
                      return {
                        ...mergeState,
                        rightSibling,
                      };
                    }
                  });
                } else {
                  return mergeState;
                }
              });

              return mergeStateResult.transform(({ borrowMergeDone, leftSibling, rightSibling }) => {
                if (borrowMergeDone) {
                  return result;
                } else {
                  // no borrowing possible, merge two child nodes
                  let deleteIndex: number | undefined = undefined;
                  let newMergedChildNode: BTreeNode | undefined = undefined;
                  if (leftSibling !== undefined) {
                    deleteIndex = index - 1;
                    newMergedChildNode = this.newNode(
                      modifications,
                      [...leftSibling.keys, newKeys[deleteIndex], ...newChildData.keys],
                      leftSibling.children &&
                        newChildData.children && {
                          ids: [...leftSibling.children.ids, ...newChildData.children.ids],
                          sizes: [...leftSibling.children.sizes, ...newChildData.children.sizes],
                        }
                    );

                    modifications.obsoleteNodes.push(leftSibling);
                  } else if (rightSibling !== undefined) {
                    deleteIndex = index;
                    newMergedChildNode = this.newNode(
                      modifications,
                      [...newChildData.keys, newKeys[deleteIndex], ...rightSibling.keys],
                      newChildData.children &&
                        rightSibling.children && {
                          ids: [...newChildData.children.ids, ...rightSibling.children.ids],
                          sizes: [...newChildData.children.sizes, ...rightSibling.children.sizes],
                        }
                    );

                    modifications.obsoleteNodes.push(rightSibling);
                  } else {
                    // cannot happen
                    throw new Error("merge not possible");
                  }

                  const newChildrenIds = this.copyAndDelete(newChildren.ids, deleteIndex);
                  const newChildrenSizes = this.copyAndDelete(newChildren.sizes, deleteIndex);
                  newChildrenIds[deleteIndex] = newMergedChildNode.id;
                  newChildrenSizes[deleteIndex] = this.nodeSize(newMergedChildNode);

                  return {
                    newChildData: {
                      keys: this.copyAndDelete(newKeys, deleteIndex),
                      children: {
                        ids: newChildrenIds,
                        sizes: newChildrenSizes,
                      },
                    },
                    deletedKey,
                  };
                }
              });
            }
          }
        });
      }
    });
  }

  deleteKey(key: string, rootId: string): Result<BTreeModificationResult> {
    this.assertString(key);
    const modifications = new Modifications();
    let newRootId = rootId;

    return this.deleteKeyInternal(key, rootId, modifications).transform((deleteResult) => {
      if (deleteResult !== undefined) {
        this.assert(deleteResult.deletedKey === key);

        if (deleteResult.newChildData.keys.length > 0 || !deleteResult.newChildData.children) {
          const newRoot = this.newNodeFromNodeData(modifications, deleteResult.newChildData);
          newRootId = newRoot.id;
        } else {
          // the tree depth is reduced by one
          this.assert(deleteResult.newChildData.children.ids.length === 1);
          newRootId = deleteResult.newChildData.children.ids[0];
        }
      }

      return new BTreeModificationResult(newRootId, modifications.newNodes, modifications.obsoleteNodes);
    });
  }
}
