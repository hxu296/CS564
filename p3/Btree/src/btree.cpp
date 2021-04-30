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
//    private:
//
//    /**
//     * File object for the index file.
//     */
//    File		*file;
//
//    /**
//     * Buffer Manager Instance.
//     */
//    BufMgr	*bufMgr;
//
//    /**
//     * Page number of meta page.
//     */
//    PageId	headerPageNum;
//
//    /**
//     * page number of root page of B+ tree inside index file.
//     */
//    PageId	rootPageNum;
//
//    /**
//     * Datatype of attribute over which index is built.
//     */
//    Datatype	attributeType;
//
//    /**
//     * Offset of attribute, over which index is built, inside records.
//     */
//    int 		attrByteOffset;
//
//    /**
//     * Number of keys in leaf node, depending upon the type of key.
//     */
//    int			leafOccupancy;
//
//    /**
//     * Number of keys in non-leaf node, depending upon the type of key.
//     */
//    int			nodeOccupancy;
//
//
//    // MEMBERS SPECIFIC TO SCANNING
//
//    /**
//     * True if an index scan has been started.
//     */
//    bool		scanExecuting;
//
//    /**
//     * Index of next entry to be scanned in current leaf being scanned.
//     */
//    int			nextEntry;
//
//    /**
//     * Page number of current page being scanned.
//     */
//    PageId	currentPageNum;
//
//    /**
//     * Current Page being scanned.
//     */
//    Page		*currentPageData;
//
//    /**
//     * Low INTEGER value for scan.
//     */
//    int			lowValInt;
//
//    /**
//     * Low DOUBLE value for scan.
//     */
//    double	lowValDouble;
//
//    /**
//     * Low STRING value for scan.
//     */
//    std::string	lowValString;
//
//    /**
//     * High INTEGER value for scan.
//     */
//    int			highValInt;
//
//    /**
//     * High DOUBLE value for scan.
//     */
//    double	highValDouble;
//
//    /**
//     * High STRING value for scan.
//     */
//    std::string highValString;
//
//    /**
//     * Low Operator. Can only be GT(>) or GTE(>=).
//     */
//    Operator	lowOp;
//
//    /**
//     * High Operator. Can only be LT(<) or LTE(<=).
//     */
//    Operator	highOp;

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
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
    // initialize BTreeIndex based on index file.
    if(BlobFile::exists(indexName)){
        // index file exists.
        BlobFile indexFile = BlobFile::open(indexName);
        file = &indexFile;
        headerPageNum = indexFile.getFirstPageNo();
        Page *metaPage;
        bufMgr->readPage(file, headerPageNum, metaPage);
        bufMgr->unPinPage(file, headerPageNum, false);
        // set up global var.
        IndexMetaInfo *metaInfo = (IndexMetaInfo*)metaPage;
        rootPageNum = metaInfo->rootPageNo;
        leafOccupancy = metaInfo->leafOccupancy;
        nodeOccupancy = metaInfo->nodeOccupancy;
        depth = metaInfo->depth;
    } else{
        // index file doesn't exist.
        BlobFile indexFile = BlobFile::create(indexName);
        file = &indexFile;
        Page *metaPage, *rootPage;
        bufMgr->allocPage(file, headerPageNum, metaPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);
        // set up global var.
        leafOccupancy = 0;
        nodeOccupancy = 0;
        depth = 0;
        // set up meta node.
        IndexMetaInfo *metaInfo = (IndexMetaInfo*)metaPage;
        strcpy(metaInfo->relationName, relationName.c_str());
        metaInfo->attrType = attributeType;
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->rootPageNo = rootPageNum;
        metaInfo->leafOccupancy = leafOccupancy;
        metaInfo->nodeOccupancy = nodeOccupancy;
        metaInfo->depth = depth;
        bufMgr->unPinPage(file, headerPageNum, true);
        // set up root node.
        LeafNodeInt *rootNode = (LeafNodeInt*)rootPage;
        rootNode->type = LEAF;
        rootNode->size = 0;
        rootNode->parenId = 0;
        bufMgr->unPinPage(file, rootPageNum, true);
        // insert all tuples to this BTree.
        FileScan fscan = FileScan(relationName, bufMgr);
        try{
            RecordId scanRid;
            while(1){
                fscan.scanNext(scanRid);
                std::string recordStr = fscan.getRecord();
                const char *record = recordStr.c_str();
                int key = *((int *)(record + attrByteOffset));
                std::cout << "DEBUG: Extracted : " << key << std::endl;
                //insertEntry(const_cast<const int*>(&key), scanRid); // TODO: type checking
            }
        } catch(const EndOfFileException &e){
            std::cout << "DEBUG: Read all records" << std::endl;
        }
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.

    // clearing up any state variables
    // TODO: not sure, what is state var
    // unpinning any B+ Tree pages that are pinned
    // TODO: which pages are pinned?
    //bufMgr->unPinPage(file);

    // flushing the index file
    bufMgr->flushFile(file);
    // deletion of the file object is required, which will call the destructor of File class causing the index file to be closed
    delete file;
}

/**
 * helper method for findTargetLeaf. Recursively find the PageId of the target leaf node.
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
    while(targetIndex < node->size && node->keyArray[targetIndex] < intKey)
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
 * Helper method for insertEntry. Return the leaf node PageId to insert the key in.
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
    for(int i = nonLeafNode->size; i > targetIndex; i--){
        nonLeafNode->keyArray[i] = nonLeafNode->keyArray[i - 1];
        nonLeafNode->pageNoArray[i + 1] = nonLeafNode->pageNoArray[i];
    }
    nonLeafNode->keyArray[targetIndex] = intKey;
    nonLeafNode->pageNoArray[targetIndex + 1] = pageNo; // todo: need to change this.
    nonLeafNode->size++;
    bufMgr->unPinPage(file, targetNonLeafId, true);
}

void BTreeIndex::insertLeaf(PageId targetLeafId, const void *key, const RecordId rid){
//    // link newNode to targetNode's parent node.
//    PageId parentPageId;
//    if(targetNode->parenId == 0){
//        // targetNode is root, allocate and setup parent page.
//        Page *parentPage;
//        bufMgr->allocPage(file, parentPageId, parentPage);
//        NonLeafNodeInt *parentNode = (NonLeafNodeInt*)parentPage;
//        parentNode->type = NONLEAF;
//        parentNode->parenId = 0;
//        parentNode->size = 0;
//        parentNode->level = 1;
//        bufMgr->unPinPage(file, parentPageId, true);
//    } else{
//        parentPageId = targetNode->parenId;
//    }
//    naiveInsertNonLeaf(parentPageId, (void *) newNode->keyArray, newLeafId); // passing the pointer to the first element of right node
}

/**
 * Assume targetLeaf is a full leaf node. Split it into two leaf nodes that belong to targetLeaf's parent.
 * @param targetLeaf
 * @param key
 * @param rid
 * @return
 */
PageId BTreeIndex::splitLeaf(PageId targetLeafId, const void *key, const RecordId rid){
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
        totalIds[i] = targetNode->ridArray[i];
    }
    for(i = 0; i < midIndex; i++){
        leftKeys[i] = totalKeys[i];
        leftIds[i] = totalIds[i];
        leftSize++;
    }
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
    bufMgr->unPinPage(file, targetLeafId, true);
    bufMgr->unPinPage(file, newLeafId, true);
    return newLeafId;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
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

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.
}

}
