/***
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

/**
   * BTreeIndex Constructor.
   * Check to see if the corresponding index file exists. If so, open the file.
   * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // Add your code below. Please do not remove this line.
    bufMgr = bufMgrIn;
    attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
    // find index file.
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str();
    outIndexName = indexName;
    // set up index file.
    std::cout << indexName << std::endl;
    if(BlobFile::exists(indexName)){
        // index file exists.
        file = new BlobFile(indexName, false);
        headerPageNum = file->getFirstPageNo();
        Page *metaPage;
        bufMgr->readPage(file, headerPageNum, metaPage);
        bufMgr->unPinPage(file, headerPageNum, false);
        // set up insert related global vars.
        IndexMetaInfo *metaInfo = (IndexMetaInfo*)metaPage;
        rootPageNum = metaInfo->rootPageNo;
        leafOccupancy = metaInfo->leafOccupancy;
        nodeOccupancy = metaInfo->nodeOccupancy;
        depth = metaInfo->depth;
    } else{
        // index file doesn't exist.
        file =  new BlobFile(indexName, true);
        Page *metaPage, *rootPage;
        bufMgr->allocPage(file, headerPageNum, metaPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);
        // set up global var.
        leafOccupancy = 0;
        nodeOccupancy = 0;
        depth = 0;
        // set up meta info.
        IndexMetaInfo *metaInfo = (IndexMetaInfo*)metaPage;
        strcpy(metaInfo->relationName, relationName.c_str());
        bufMgr->unPinPage(file, headerPageNum, true);
        // set up root node.
        LeafNodeInt *rootNode = (LeafNodeInt*)rootPage;
        rootNode->type = LEAF;
        rootNode->size = 0;
        rootNode->parentId = MAX_PAGEID;
        rootNode->rightSibPageNo = MAX_PAGEID;
        bufMgr->unPinPage(file, rootPageNum, true);
    }
    // insert all tuples in relation file to this BTree.
    FileScan fscan = FileScan(relationName, bufMgr);
    try{
        RecordId scanRid;
        while(1){
            fscan.scanNext(scanRid);
            std::string recordStr = fscan.getRecord();
            const char *record = recordStr.c_str();
            int key = *((int *)(record + attrByteOffset));
            insertEntry(const_cast<const int*>(&key), scanRid);
        }
    } catch(const EndOfFileException &e){
    }
    // set up scan related global vars.
    currentPageNum = MAX_PAGEID;
    currentPageData = nullptr;
    nextEntry = -1;
    scanExecuting = false;
}

/**
* BTreeIndex Destructor.
* End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
* and delete file instance thereby closing the index file.
* Destructor should not throw any exceptions. All exceptions should be caught in here itself.
* */
BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.
    // end possible scan.
    if(scanExecuting) endScan();
    // update meta page.
    Page *metaPage;
    bufMgr->readPage(file, headerPageNum, metaPage);
    IndexMetaInfo *metaInfo = (IndexMetaInfo*)metaPage;
    metaInfo->attrType = attributeType;
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->rootPageNo = rootPageNum;
    metaInfo->leafOccupancy = leafOccupancy;
    metaInfo->nodeOccupancy = nodeOccupancy;
    metaInfo->depth = depth;
    bufMgr->unPinPage(file, headerPageNum, true);
    // flush index file.
    bufMgr->flushFile(file);
    // delete index file.
    delete file;
}

/**
 * Print the statistics about this Tree.
 */
void BTreeIndex::printTreeStatus(){
    printf("Tree Status: \n");
    Page *metaPage;
    bufMgr->readPage(file, headerPageNum, metaPage);
    bufMgr->unPinPage(file, headerPageNum, false);
    IndexMetaInfo *metaInfo = (IndexMetaInfo*)metaPage;
    printf("\trelationName: %s\n", metaInfo->relationName);
    printf("\tdepth: %d\n", depth);
    printf("\theaderPageNum: %d\n", headerPageNum);
    printf("\trootPageNum: %d\n", rootPageNum);
    printf("\tleafOccupancy: %d\n", leafOccupancy);
    printf("\tnodeOccupancy: %d\n", nodeOccupancy);
    printf("\tINTARRAYNONLEAFSIZE: %d\n", INTARRAYNONLEAFSIZE);
    printf("\tINTARRAYLEAFSIZE: %d\n", INTARRAYLEAFSIZE);
}

/**
 * Print the statistics of a node. Recognize node type within method.
 * @param id
 * @param page
 */
    void BTreeIndex::printNode(PageId id, Page* page){
        Nodetype type = *(Nodetype*)page;
        if(type == NONLEAF){
            NonLeafNodeInt *node = (NonLeafNodeInt*)page;
            printf("Internal node %d Stats:\n", id);
            printf("\tsize: %d\n", node->size);
            if(node->size != 0)
                printf("\tminKey: %d maxKey: %d\n", node->keyArray[0], node->keyArray[node->size - 1]);
            printf("\tkeyArray: ");
            for(int i = 0; i < node->size; i++)
                printf("%d ", node->keyArray[i]);
            printf("\n");
            printf("\tchildren: ");
            for(int i = 0; i < node->size + 1; i++)
                printf("%d ", node->pageNoArray[i]);
            printf("\n");
        } else{
            LeafNodeInt *node = (LeafNodeInt*)page;
            printf("Leaf node %d Stats:\n", id);
            printf("\tsize: %d\n", node->size);
            if(node->size != 0)
                printf("\tminKey: %d maxKey: %d\n", node->keyArray[0], node->keyArray[node->size - 1]);
            printf("\trightSib: %d\n", node->rightSibPageNo);
            printf("\tkeyArray: ");
            for(int i = 0; i < node->size; i++)
                printf("%d ", node->keyArray[i]);
            printf("\n");
        }
    }

/**
 * Recursively find the PageId of the target leaf node.
 * @param pageId
 * @param key
 * @return
 */
PageId BTreeIndex::findTargetLeafHelper(PageId pageId, const void *key){
    Page *page;
    bufMgr->readPage(file, pageId, page);
    bufMgr->unPinPage(file, pageId, false);
    NonLeafNodeInt* node = ((NonLeafNodeInt*)page);
    PageId targetPageId;

    // targetIndex should meet the following condition: keyArray[targetIndex] >= key > keyArray[targetIndex-1].
    int targetIndex = 0, intKey = *(int*)key;
    while(targetIndex < node->size && node->keyArray[targetIndex] <= intKey)
        targetIndex++;

    if(node->level == 1){
        // base case.
        targetPageId = node->pageNoArray[targetIndex];
    } else{
        // recursive case.
        targetPageId = findTargetLeafHelper(node->pageNoArray[targetIndex], key);
    }

    return targetPageId;
}

/**
 * Return the leaf node PageId to insert the key in.
 * @param key
 * @return Leaf node PageId to insert the key in.
 */
PageId BTreeIndex::findTargetLeaf(const void *key){
    if(depth == 0)
        return rootPageNum;
    else
        return findTargetLeafHelper(rootPageNum, key);
}

/**
 * Checks whether the node is full.
 * @param node
 * @return True if the node is full, false otherwise.
 */
bool BTreeIndex::isFull(PageId targetId){
    Page *node;
    bufMgr->readPage(file, targetId, node);
    bufMgr->unPinPage(file, targetId, false);
    Nodetype type = *(Nodetype*)node; // Nodetype is the first attribute for both types of nodes.
    if(type == NONLEAF)
        return ((NonLeafNodeInt*)node)->size == INTARRAYNONLEAFSIZE;
    else
        return ((LeafNodeInt*)node)->size == INTARRAYLEAFSIZE;
}

/**
 * Assume leaf is not full. Insert the key-rid pair into the leaf.
 * @param node
 * @param key
 * @param rid
 */
void BTreeIndex::naiveInsertLeaf(PageId targetLeafId, const void *key, const RecordId rid){
    Page *targetLeaf;
    bufMgr->readPage(file, targetLeafId, targetLeaf);
    LeafNodeInt *leafNode = (LeafNodeInt*)targetLeaf;
    // find the target index that maintains the ascending order upon inserting new key.
    int targetIndex = 0, intKey = *(int*)key;
    while(targetIndex < leafNode->size && leafNode->keyArray[targetIndex] < intKey)
        targetIndex++;
    // insert key and rid to targetIndex.
    for(int i = leafNode->size; i > targetIndex; i--){
        leafNode->keyArray[i] = leafNode->keyArray[i - 1];
        leafNode->ridArray[i] = leafNode->ridArray[i - 1];
    }
    leafNode->keyArray[targetIndex] = intKey;
    leafNode->ridArray[targetIndex] = rid;
    leafNode->size++;
    bufMgr->unPinPage(file, targetLeafId, true);
    leafOccupancy++;
}

/**
 * Assume nonleaf is not full, Insert the key-pageNo pair into the nonleaf.
 * @param targetNonLeaf
 * @param key
 * @param pageNo
 */
void BTreeIndex::naiveInsertNonLeaf(PageId targetNonLeafId, const void *key, PageId pageNo){
    Page *targetNonLeaf;
    bufMgr->readPage(file, targetNonLeafId, targetNonLeaf);
    NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt*)targetNonLeaf;
    // find the target index that maintains the ascending order upon inserting new key.
    int targetIndex = 0, intKey = *(int*)key;
    while(targetIndex < nonLeafNode->size && nonLeafNode->keyArray[targetIndex] < intKey)
        targetIndex++;
    // inserting key and pageNo to targetIndex and targetIndex + 1, respectively.
    // assume minKeyIn(pageNo) > maxKeyIn(pageNoArray[targetIndex]).
    for (int i = nonLeafNode->size; i > targetIndex; i--) {
        nonLeafNode->keyArray[i] = nonLeafNode->keyArray[i - 1];
        nonLeafNode->pageNoArray[i + 1] = nonLeafNode->pageNoArray[i];
    }
    nonLeafNode->keyArray[targetIndex] = intKey;
    nonLeafNode->pageNoArray[targetIndex + 1] = pageNo;
    nonLeafNode->size++;
    bufMgr->unPinPage(file, targetNonLeafId, true);
    nodeOccupancy++;
}

/**
 * Insert key, leftPageNo, and rightPageNo to the empty (newly created) root. All keys in leftPage are smaller than
 * those in the right page.
 * @param key
 * @param leftPageNo
 * @param rightPageNo
 */
void BTreeIndex::insertNewRoot(const void *key, PageId leftPageNo, PageId rightPageNo){
    Page *rootPage;
    int intKey = *(int*)key;
    bufMgr->readPage(file, rootPageNum, rootPage);
    NonLeafNodeInt *rootNode = (NonLeafNodeInt*)rootPage;
    rootNode->keyArray[0] = intKey;
    rootNode->pageNoArray[0] = leftPageNo;
    rootNode->pageNoArray[1] = rightPageNo;
    rootNode->size++;
    bufMgr->unPinPage(file, rootPageNum, true);
    nodeOccupancy++;
}

/**
 * Assume nonleaf is full. Insert key-pageNo pair and perform recursive split.
 * @param targetNonLeafId
 * @param key
 * @param pageNo
 */
void BTreeIndex::insertNonLeaf(PageId targetNonLeafId, const void *key, PageId pageNo){
    printf("inserting NonLeaf, target id: %d\n", targetNonLeafId);
    // create newNonLeaf
    Page *targetNonLeaf, *newNonLeaf;
    PageId newNonLeafId;
    bufMgr->readPage(file, targetNonLeafId, targetNonLeaf);
    bufMgr->allocPage(file, newNonLeafId, newNonLeaf);
    NonLeafNodeInt *targetNode = (NonLeafNodeInt*)targetNonLeaf;
    NonLeafNodeInt *newNode = (NonLeafNodeInt*)newNonLeaf;
    newNode->type = NONLEAF;
    newNode->level = targetNode->level;
    //printNode(targetNonLeafId, targetNonLeaf);
    // split keys into left keys, mid key, and right keys.
    int midIndex = (INTARRAYNONLEAFSIZE + 1) / 2;
    int leftKeys[INTARRAYNONLEAFSIZE], rightKeys[INTARRAYNONLEAFSIZE], totalKeys[INTARRAYNONLEAFSIZE + 1];
    int leftSize = 0, rightSize = 0, intKey = *(int*)key, i, j;
    for(i = 0; i < INTARRAYNONLEAFSIZE && targetNode->keyArray[i] < intKey; i++)
        totalKeys[i] = targetNode->keyArray[i];
    totalKeys[i] = intKey;
    for(; i < INTARRAYNONLEAFSIZE; i++)
        totalKeys[i + 1] = targetNode->keyArray[i];
    for(i = 0; i < midIndex; i++) {
        leftKeys[i] = totalKeys[i];
        leftSize++;
    }
    int *midKey = new int;
    *midKey = totalKeys[i++];
    for(j = i; j < INTARRAYNONLEAFSIZE + 1; j++) {
        rightKeys[j - i] = totalKeys[j];
        rightSize++;
    }
    // split pageNo into left pageNo and right pageNo.
    int leftPageNo[INTARRAYNONLEAFSIZE + 1], rightPageNo[INTARRAYNONLEAFSIZE + 1], totalPageNo[INTARRAYNONLEAFSIZE + 2];
    for(i = 0; i < INTARRAYNONLEAFSIZE + 1 && targetNode->keyArray[i] < intKey; i++)
        totalPageNo[i] = targetNode->pageNoArray[i];
    totalPageNo[i] = pageNo; //   i = insert key index + 1
    for(; i < INTARRAYNONLEAFSIZE + 1; i++)
        totalPageNo[i + 1] = targetNode->pageNoArray[i];
    for(i = 0; i <= midIndex; i++)
        leftPageNo[i] = totalPageNo[i];
    for(j = i; j < INTARRAYNONLEAFSIZE + 2; j++)
        rightPageNo[j - i] = totalPageNo[j];
    // assign left to targetNonLeaf, right to newNonLeaf
    targetNode->size = leftSize;
    std::copy(std::begin(leftKeys), std::end(leftKeys), std::begin(targetNode->keyArray));
    std::copy(std::begin(leftPageNo), std::end(leftPageNo), std::begin(targetNode->pageNoArray));
    newNode->size = rightSize;
    std::copy(std::begin(rightKeys), std::end(rightKeys), std::begin(newNode->keyArray));
    std::copy(std::begin(rightPageNo), std::end(rightPageNo), std::begin(newNode->pageNoArray));
        //printNode(targetNonLeafId, targetNonLeaf);
        //printNode(newNonLeafId, newNonLeaf);
    // change the parent entry for children in right node.
    for(i = 0; i < rightSize + 1; i++){
        Page *childPage;
        PageId childPageId = newNode->pageNoArray[i];
        bufMgr->readPage(file, childPageId, childPage);
        if(targetNode->level == 0){
            NonLeafNodeInt* childNode = (NonLeafNodeInt*)childPage;
            childNode->parentId = newNonLeafId;
        } else{
            LeafNodeInt* childNode = (LeafNodeInt*)childPage;
            childNode->parentId = newNonLeafId;
        }
        bufMgr->unPinPage(file, childPageId, true);
    }
    nodeOccupancy++;
    // find parent for targetNonLeaf and newNonLeaf.
    PageId parentPageId;
    if(targetNode->parentId == MAX_PAGEID){
        // targetNode is root, allocate and setup parent page.
        Page *parentPage;
        bufMgr->allocPage(file, parentPageId, parentPage);
        NonLeafNodeInt *parentNode = (NonLeafNodeInt*)parentPage;
        parentNode->type = NONLEAF;
        parentNode->parentId = MAX_PAGEID;
        parentNode->size = 0;
        parentNode->level = 0;
        bufMgr->unPinPage(file, parentPageId, true);
        rootPageNum = parentPageId;
        depth++;
        // link targetNonLeaf and newNonLeaf to parent.
        targetNode->parentId = newNode->parentId = parentPageId;
        bufMgr->unPinPage(file, targetNonLeafId, true);
        bufMgr->unPinPage(file, newNonLeafId, true);
        // insert mid key to parent.
        insertNewRoot(midKey, targetNonLeafId, newNonLeafId);
    } else{
        parentPageId = targetNode->parentId;
        // link targetNonLeaf and newNonLeaf to parent.
        newNode->parentId = parentPageId;
        bufMgr->unPinPage(file, targetNonLeafId, true);
        bufMgr->unPinPage(file, newNonLeafId, true);
        // insert mid key to parent.
        if(isFull(parentPageId)){
            insertNonLeaf(parentPageId, midKey, newNonLeafId); // todo: passing stack pointer to another call.
        } else {
            naiveInsertNonLeaf(parentPageId, midKey, newNonLeafId);
        }
    }
}

/**
 * Assume leaf is full. Insert key-rid pair and perform recursive split.
 * @param targetLeafId
 * @param key
 * @param rid
 */
void BTreeIndex::insertLeaf(PageId targetLeafId, const void *key, const RecordId rid){
    Page *targetLeaf, *newLeaf;
    PageId newLeafId;
    bufMgr->readPage(file, targetLeafId, targetLeaf);
    bufMgr->allocPage(file, newLeafId, newLeaf);
    LeafNodeInt *targetNode = (LeafNodeInt*)targetLeaf;
    LeafNodeInt *newNode = (LeafNodeInt*)newLeaf;
    newNode->type = LEAF;
    int midIndex = (INTARRAYLEAFSIZE + 1) / 2;
    // split keys and ids into left keys, right keys, and left ids, right ids, where right is bigger than left.
    int leftKeys[INTARRAYLEAFSIZE], rightKeys[INTARRAYLEAFSIZE], totalKeys[INTARRAYLEAFSIZE + 1];
    int leftSize = 0, rightSize = 0, intKey = *(int*)key, i, j;
    RecordId leftIds[INTARRAYLEAFSIZE], rightIds[INTARRAYLEAFSIZE], totalIds[INTARRAYLEAFSIZE + 1];
    for(i = 0; i < INTARRAYLEAFSIZE && targetNode->keyArray[i] < intKey; i++) {
        totalKeys[i] = targetNode->keyArray[i];
        totalIds[i] = targetNode->ridArray[i];
    }
    totalKeys[i] = intKey;
    totalIds[i] = rid;
    for(; i < INTARRAYLEAFSIZE; i++){
        totalKeys[i + 1] = targetNode->keyArray[i];
        totalIds[i + 1] = targetNode->ridArray[i];
    }
    for(i = 0; i < midIndex; i++){
        leftKeys[i] = totalKeys[i];
        leftIds[i] = totalIds[i];
        leftSize++;
    }
    int *midKey = new int;
    *midKey = totalKeys[i];
    for(j = i; j < INTARRAYLEAFSIZE + 1; j++){
        rightKeys[j - i] = totalKeys[j];
        rightIds[j - i] = totalIds[j];
        rightSize++;
    }
    // let targetNode be the left node, newNode be the right node.
    targetNode->size = leftSize;
    std::copy(std::begin(leftKeys), std::end(leftKeys), std::begin(targetNode->keyArray));
    std::copy(std::begin(leftIds), std::end(leftIds), std::begin(targetNode->ridArray));
    newNode->size = rightSize;
    std::copy(std::begin(rightKeys), std::end(rightKeys), std::begin(newNode->keyArray));
    std::copy(std::begin(rightIds), std::end(rightIds), std::begin(newNode->ridArray));
    newNode->rightSibPageNo = targetNode->rightSibPageNo;
    targetNode->rightSibPageNo = newLeafId;
    leafOccupancy++;
    // link newNode to targetNode's parent node.
    PageId parentPageId;
    if(targetNode->parentId == MAX_PAGEID){
        // targetNode is root, allocate and setup parent page.
        Page *parentPage;
        bufMgr->allocPage(file, parentPageId, parentPage);
        NonLeafNodeInt *parentNode = (NonLeafNodeInt*)parentPage;
        parentNode->type = NONLEAF;
        parentNode->parentId = MAX_PAGEID;
        parentNode->size = 0;
        parentNode->level = 1;
        bufMgr->unPinPage(file, parentPageId, true);
        rootPageNum = parentPageId;
        depth++;
        // link targetNonLeaf and newNonLeaf to parent.
        targetNode->parentId = newNode->parentId = parentPageId;
        bufMgr->unPinPage(file, targetLeafId, true);
        bufMgr->unPinPage(file, newLeafId, true);
        // insert midKey to parent.
        insertNewRoot(midKey, targetLeafId, newLeafId);
    } else{
        parentPageId = targetNode->parentId;
        newNode->parentId = parentPageId;
        bufMgr->unPinPage(file, targetLeafId, true);
        bufMgr->unPinPage(file, newLeafId, true);
        if(isFull(parentPageId)){
            insertNonLeaf(parentPageId, midKey, newLeafId);
        } else {
            naiveInsertNonLeaf(parentPageId, midKey, newLeafId); // todo: check
        }
    }
}

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
* @param key			Key to insert, pointer to integer/double/char string
* @param rid			Record ID of a record whose entry is getting inserted into the index.
**/
void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
    PageId targetLeafId = findTargetLeaf(key);
    if(isFull(targetLeafId)){
        insertLeaf(targetLeafId, key, rid);
    }else{
        naiveInsertLeaf(targetLeafId, key, rid);
    }
}

/**
    * Find the index of the given key in leaf node
    * @param key
    * @param pageNo
    */
int BTreeIndex::searchHelper(const void *key, LeafNodeInt* node, LeafNodeInt*& new_node, PageId& new_id)
{
    int targetIndex = -1;

    // the node found is the last node, no right sibling
    for (int i = 0; i < node->size; i++) {
        if (node->keyArray[i] >= *((int*)key)) {
            targetIndex = i;
            break;
        }
    }

    // if -1, move on to next page
    while (targetIndex == -1 && node->rightSibPageNo != MAX_PAGEID) {
        // update node
        bufMgr->unPinPage(file, ((Page*)node)->page_number(), false);
        currentPageNum = node->rightSibPageNo;
        Page *rightSibpage;
        bufMgr->readPage(file, node->rightSibPageNo, rightSibpage);
        node = (LeafNodeInt*)rightSibpage;
        new_node = node; // return the new node TODO
        new_id = node->rightSibPageNo;

        for (int i = 0; i < node->size; i++) {
            if (node->keyArray[i] >= *((int*)key)) {
                targetIndex = i;
                break;
            }
        }
    }

    return targetIndex;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

/**
 * Begin a filtered scan of the index.  For instance, if the method is called
 * using ("a",GT,"d",LTE) then we should seek all entries with a value
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
* @param lowVal	Low value of range, pointer to integer / double / char string
* @param lowOp		Low operator (GT/GTE)
* @param highVal	High value of range, pointer to integer / double / char string
* @param highOp	High operator (LT/LTE)
* @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
* @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
**/
void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
//    printTreeStatus();
//    Page *page1;
//    bufMgr->readPage(file, 600, page1);
//    bufMgr->unPinPage(file, 600, false);
//    printNode(600, page1);
//    Page *page2;
//    bufMgr->readPage(file, 677, page2);
//    bufMgr->unPinPage(file, 677, false);
//    printNode(677, page2);
     //Add your code below. Please do not remove this line.
    std::cout << "Here start scanning"<< std::endl;
    // convert the input to int (assumed only use integer)
    lowValInt = *((int*)lowValParm);
    highValInt = *((int*)highValParm);
    lowOp = lowOpParm;
    highOp = highOpParm;

    // low value should be less than or equal to high value
    if (lowValInt > highValInt)
        throw BadScanrangeException();

    // only support GT, GTE and LT, LTE operators
    if (lowOp != GT && lowOp != GTE)
        throw BadOpcodesException();
    if (highOp != LT && highOp != LTE)
        throw BadOpcodesException();

    // If another scan is already executing, that needs to be ended here.
    if (scanExecuting) {
        endScan();
        return;
    }

    // Start from root to find out the leaf page that contains the first RecordID
    // that satisfies the scan parameters. Keep that page pinned in the buffer pool.

    // first find the page that may contain first rid in given range
    PageId target_page_id = findTargetLeaf(lowValParm); // this returns the page may contain the target key
    Page *page;
    printf("target id: %d\n", target_page_id);
    bufMgr->readPage(file, target_page_id, page);
    LeafNodeInt* curr_leaf_node = ((LeafNodeInt*)page);
    LeafNodeInt* target_node = curr_leaf_node; // store the updated node after search

    // we then do the search to see if it really exists
    int key_index = searchHelper(lowValParm, curr_leaf_node, target_node,target_page_id);
    if (key_index == -1) {
        bufMgr->unPinPage(file, target_page_id, false); // unpin page no longer useful
        throw NoSuchKeyFoundException();
    }
    else { // found
        if (lowOp == GTE) {
            currentPageNum = target_page_id;
            currentPageData = (Page*)target_node;
            nextEntry = key_index;
        }
        if (lowOp == GT) {
            // if first key returned equals the lower bound, next key is the target
            if (target_node->keyArray[key_index] == lowValInt) {
                // next key is in next entry
                if (key_index == target_node->size - 1) {
                    // pin the right sibling (rightSibPageNo)
                    std::cout << "Target page id is " << target_page_id << std::endl;
                    std::cout << "Key index is " << key_index << std::endl;
                    std::cout << "Try unpinning in GT"<< std::endl;
                    bufMgr->unPinPage(file, target_page_id, false);
                    std::cout << "After Unpinning"<< std::endl;
                    currentPageNum = target_node->rightSibPageNo;
                    Page *rightSibpage;
                    if(target_node->rightSibPageNo != MAX_PAGEID){
                        bufMgr->readPage(file, target_node->rightSibPageNo, rightSibpage);
                        currentPageData = rightSibpage;
                        nextEntry = 0;
                    }
                }
                else {
                    currentPageNum = target_page_id;
                    currentPageData = (Page*)target_node;
                    nextEntry = key_index + 1;
                }
            }
            else { // returned one is correct one
                currentPageNum = target_page_id;
                currentPageData = (Page*)target_node;
                nextEntry = key_index;
            }
        }
    }
    scanExecuting = true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

/**
 * Fetch the record id of the next index entry that matches the scan.
 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
* @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
**/
void BTreeIndex::scanNext(RecordId& outRid)
{
    // Add your code below. Please do not remove this line.
    if(!scanExecuting)
        throw ScanNotInitializedException();

    // check if currentPageNum is valid
    LeafNodeInt* curr_node = curr_node = (LeafNodeInt*)currentPageData;


    // get info for next entry to scan
    int key = curr_node->keyArray[nextEntry];

    if (highOp == LT) {
        if (key < highValInt) {
            outRid = curr_node->ridArray[nextEntry];

            // update nextEntry
            if (nextEntry == curr_node->size - 1) {
                bufMgr->unPinPage(file, currentPageNum, false);
                currentPageNum = curr_node->rightSibPageNo;
                Page *rightSibpage;
                if(curr_node->rightSibPageNo != MAX_PAGEID){
                    bufMgr->readPage(file, curr_node->rightSibPageNo, rightSibpage);
                    currentPageData = rightSibpage;
                    nextEntry = 0;
                }
                else {
                    throw IndexScanCompletedException();
                }
            }
            else {
                nextEntry++;
            }
        }
        else {
            throw IndexScanCompletedException();
        }
    }

    if (highOp == LTE) {
        if (key <= highValInt) {
            outRid = curr_node->ridArray[nextEntry];

            // update nextEntry
            if (nextEntry == curr_node->size - 1) {
                bufMgr->unPinPage(file, currentPageNum, false);
                currentPageNum = curr_node->rightSibPageNo;
                Page *rightSibpage;
                if(curr_node->rightSibPageNo != MAX_PAGEID){
                    bufMgr->readPage(file, curr_node->rightSibPageNo, rightSibpage);
                    currentPageData = rightSibpage;
                    nextEntry = 0;
                }
                else {
                    throw IndexScanCompletedException();
                }
            }
            else {
                nextEntry++;
            }
        }
        else {
            throw IndexScanCompletedException();
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//

/**
 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
 * @throws ScanNotInitializedException If no scan has been initialized.
**/
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.

    if (!scanExecuting)
        throw ScanNotInitializedException();

    // unpins all the pages that have been pinned for the purpose of the scan

    bufMgr->unPinPage(file, currentPageNum, false);
    // clear the relative fields // TODO
    currentPageNum = MAX_PAGEID;
    currentPageData = nullptr;
    nextEntry = -1;
    scanExecuting = false;
}

}
