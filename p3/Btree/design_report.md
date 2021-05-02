# Design Report

## 1. Introduction
In this report, I will group our design choices into 3 categories and illustrate them one by one. In the BufMgr section, I will defense why we choose to un-pin each page immediately after use. In the Node and Tree sections, I will talk about how node attributes are designed and how that interacts with the tree insert and traverse performance. Let's jump right in. 
## 2. BufMgr
One design choice we made was to un-pin a page immediately after use. Specifically, we used the following routine:
```c++
// read and not modify
PageId targetPageId;
Page *targetPage; 
bufMgr->readPage(file, targetPageId, targetPage)
bufMgr->unPinPage(file, targetPageId, false);
```
```c++
// read and modify
PageId targetPageId;
Page *targetPage;
bufMgr->readPage(file, targetPageId, targetPage)
// ... modify the page content  ... //
bufMgr->unPinPage(file, targetPageId, true);
```
By doing this, we consciously avoided buffer slots from being locked and thereby allowed a greater number of frequently-used pages to be saved in memory. This design leads to improved I/O performance at cost of CPU run time. However, because BufMgr uses the clock algorithm, resolving a page miss takes only linear time. I argue that the CPU time cost is well worth the improvement on I/O performance. 
## 3. Node 
There are 3 challenges for designing a good B+ Tree node:
1. How to differentiate leaf nodes from non-leaf nodes? 
2. Say we want to split when inserting to a full node. How do we know if a node is full or not?
3. Say if we want to insert to a node's parent after splitting it. How do we know a node's parent?  

To resolve these challenges, we introduced the following node attributes: 

```c++
nodeType type; // enum nodeType{NONLEAF, LEAF}
int size; // number of keys in this node
PageId parent; // parent node's pageId. If this node is root, parent = MAX_PAGEID
```
We put the nodeType attribute as the first attribute for both LeafNodeInt and NonLeafNodeInt. Hence, casting a page pointer to a nodeType pointer and getting its value will tell us whether a page is a leaf or a non-leaf.  
The size attribute can tell us whether a node is full. In the actual implementation, we designate the `boolean isFull(PageId pageId)` helper method to do this for us.  
The parent attribute is pretty much self-explanatory. The only worth-noting part is that the root node's parent is set to a very large global, `MAX_PAGEID`, for the sake of differentiating the root from other nodes. `MAX_PAGEID` is also the value of rightSibPageNo attribute for the right-most leaf node, indicating that we have reached the end of linked list.  
Finally, I want to point out that instead of using the maximum `INTARRAYLEAFSIZE` and `INTARRAYNONLEAFSIZE`. We have to decrease them by 1 to prevent some weird overflow issue. The performance loss here is neglectable. 
## 4. Tree 

### 4a. How to grow a B+ Tree?
Upon initializing an empty tree, the root node will be a leaf node that takes key-rid pairs. Things start to get interesting when we are trying to insert to a full root. When we are trying to insert to a full leaf node, the node would split into two leaf nodes linked to the same non-leaf parent node. When splitting it, the original full node will become the left node and the newly allocated node will become the right node. The left node will be pointing to the right node, and the right node points to what left node originally points to. Keys that are smaller than the medium key will be given to the left node, and the rest given to the right node, attached with their corresponding rids. After reassigning keys and rids, two leaf nodes will be linked to a shared parent. The aforementioned medium key will be inserted to their parent, along with a pointer to the right node. An edge case is that the leaf-node-to-split does not have a parent node. This means we are inserting to the root. In this case, a non-leaf root node will be allocated and initialized to be their parent. This time, medium key, pointer to the left node, and pointer to the right node will be inserted to the root at once.  
Inserting to a non-leaf is largely similar to inserting to a leaf. The way we handle the edge case is slightly different, where this time we initialize the root's level to 0 rather than 1. A worth-noting part is that after splitting the children into 2 parts, we need to change the parent pointer for all the children of the right node to point to the right node. This requires using BufMgr to access all children nodes and modify their attribute, which can be a costly operation. 
### 4b. Traversing the Tree
Because leaves of the tree form a linked list with keys sorted in the ascending order. Traversing the tree by a range would only require one top-down traversal. Then, we just need to follow the link list until reaching the upperbound or seeing a node pointing to nothing (`MAX_PAGEID`). Theoretically, traversal would require depth + 1 I/O accesses, but because BufMgr would keep the most-recently accessed pages available, the actual I/O cost should be much better than theory.

