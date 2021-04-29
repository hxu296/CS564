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
        Page metaPage = indexFile.readPage(headerPageNum);
        IndexMetaInfo *metaInfo = (IndexMetaInfo*)&metaPage;
        rootPageNum = metaInfo->rootPageNo;
        leafOccupancy = metaInfo->leafOccupancy;
        nodeOccupancy = metaInfo->nodeOccupancy;
    } else{
        // index file doesn't exist.
        BlobFile indexFile = BlobFile::create(indexName);
        file = &indexFile;
        Page metaPage = indexFile.allocatePage(headerPageNum);
        indexFile.allocatePage(rootPageNum);
        IndexMetaInfo *metaInfo = (IndexMetaInfo*)&metaPage;
        strcpy(metaInfo->relationName, relationName.c_str());
        metaInfo->attrType = attrType;
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->rootPageNo = rootPageNum;
        // insert all tuples to this BTree.
        leafOccupancy = 0;
        nodeOccupancy = 0;
        FileScan fscan = FileScan(relationName, bufMgr);
        try{
            RecordId scanRid;
            while(1){
                fscan.scanNext(scanRid);
                std::string recordStr = fscan.getRecord();
                const char *record = recordStr.c_str();
                int key = *((int *)(record + attrByteOffset));
                //std::cout << "DEBUG: Extracted : " << key << std::endl;
                insertEntry(const_cast<const int*>(&key), scanRid); // TODO: type checking
            }
        } catch(const EndOfFileException &e){
            //std::cout << "DEBUG: Read all records" << std::endl;
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

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
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
