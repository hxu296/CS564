/**
 * Huan Xu, 9081323736, hxu296
 * Jing Zhang, 9080309488, jzhang999
 *
 * Provides implementations of the BufMgr class.
 *
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

/**
* Constructor of BufMgr class
 */
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


/**
* Destructor of BufMgr class
 */
BufMgr::~BufMgr() 
{
	// flush all dirty pages
	for (FrameId i = 0; i < numBufs; i++) {
        bufDescTable[i].pinCnt = 0;
		if (bufDescTable[i].valid && bufDescTable[i].dirty) {
            bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
            bufDescTable[i].dirty = false;
        }
  	}
	// deallocates hashtable, buffer pool, and BufDesc table
	delete hashTable;
	delete[] bufPool;
	delete[] bufDescTable;
}


/**
* Advance clock to next frame in the buffer pool
*/
void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}


/**
 * Allocate a free frame.
 *
 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
 * @throws BufferExceededException If no such buffer is found which can be allocated
 */
void BufMgr::allocBuf(FrameId & frame) 
{
    advanceClock();
    FrameId startID = clockHand; // record start frame id
    uint32_t numPinned = 0; // record number of pinned frames
    while(true){
        if (bufDescTable[clockHand].valid){
            // valid is set
            // keep track of the number of pinned frames. If all frames are pinned, throw BufferExceededException
            if(numPinned == numBufs && startID == clockHand)
                throw BufferExceededException();
            if(bufDescTable[clockHand].pinCnt > 0)
                numPinned++;
            if (bufDescTable[clockHand].refbit){
                // refbit is set, flip refbit and continue
                bufDescTable[clockHand].refbit = false;
                advanceClock();
                continue;
            }
            else{
                // refbit is not set, check whether we can use this frame
                if (bufDescTable[clockHand].pinCnt > 0){
                    // frame is pinned, can't use this frame. Advance clock and continue.
                     advanceClock();
                     continue;
                }
                else{
                    // frame is not pinned. Can use this frame
                    if(bufDescTable[clockHand].dirty){
                        // if the frame to use is dirty, flush it to disk
                        bufDescTable[clockHand].file->writePage(bufPool[bufDescTable[clockHand].frameNo]);
                        bufDescTable[clockHand].dirty = false;
                    }
                    // remove its corresponding page from the hashtable
                    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                    // clear the old frame slot for the new frame
                    bufDescTable[clockHand].Clear();
                    frame = clockHand;
                    return;
                }
            }
        } else{
            // valid is not set for the current frame. Can use this frame
            frame = clockHand;
            return;
        }
    }
}


/**
 * Lookup frameNo according to file and pageNo. Return true if a correspondence exists, false otherwise.
 * @param file Part of key for finding the frameNo.
 * @param pageNo Part of key for finding the frameNo.
 * @param frameNo FrameId to assign.
 * @return True if file and pageNo corresponds to a frameNo in hashtable, false otherwise.
 */
bool BufMgr::findFrameNo(File* file, const PageId pageNo, FrameId& frameNo){
    try {
        hashTable->lookup(file, pageNo, frameNo);
        return true; // no exception thrown. Lookup succeeds
    } catch (const HashNotFoundException &e) {
        return false; // exception occurred. Key is not in hashtable. Lookup fails
    }
}


/**
 * Reads the given page from the file into a frame and returns the pointer to page.
 * If the requested page is already present in the buffer pool pointer to that frame is returned
 * otherwise a new frame is allocated from the buffer pool for reading the page.
 *
 * @param file   	File object
 * @param PageNo  Page number in the file to be read
 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 * @throws InvalidPageException If the page to read does not exist.
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId	frameNo;
    bool found = findFrameNo(file, pageNo, frameNo);

	if (!found) {  // page to read is not in the buffer
        // read the page from disk. InvalidPageException will be thrown if page doesn't exist
	    Page page_obj = file->readPage(pageNo);
		// read succeeds. Allocate new page into the buffer
        allocBuf(frameNo);
        hashTable->insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file, pageNo);
        bufPool[frameNo] = page_obj;
        // return its corresponding page pointer in the buffer pool
        page = bufPool + frameNo;
	}
	else {  // page to read is in the buffer
	    // set refbit and increase pin count
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		// return a pointer to the frame containing the page
		page = bufPool + frameNo;
	}
}


/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 *
 * @param file   	File object
 * @param PageNo  Page number
 * @param dirty		True if the page to be unpinned needs to be marked dirty
* @throws  PageNotPinnedException If the page is not already pinned
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	// find the frameNo of the page to unpin
    FrameId	frameNo;
	bool found = findFrameNo(file, pageNo, frameNo);

	// if not found, do nothing
	if (!found) return;

	// if the pin count is already 0, throw PageNotPinnedException
	if (bufDescTable[frameNo].pinCnt == 0)
		throw PageNotPinnedException(file->filename(), pageNo, frameNo);

    // page is found and its pin count is larger than 0, decrease the pin count
    bufDescTable[frameNo].pinCnt--;

    // set dirty bit accordingly
    if (dirty) {
        bufDescTable[frameNo].dirty = true;
    }
}


/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo    Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	// allocate a new frame for the page
	FrameId	frameNo;
	allocBuf(frameNo);

	// assign a new page into the newly allocated page frame
    bufPool[frameNo] = file->allocatePage();
    pageNo = bufPool[frameNo].page_number();

	// insert new entry into the hash table
	hashTable->insert(file, pageNo, frameNo);

	// set the frame
	bufDescTable[frameNo].Set(file, pageNo);

	// return the pointer to the page in the buffer pool
    page = &bufPool[frameNo];
}


/**
 * Delete page from file and also from buffer pool if present.
 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
 *
 * @param file   	File object
 * @param pageNo  Page number
 */
void BufMgr::disposePage(File* file, const PageId pageNo)
{
    // find the frameNo of the page to dispose
    FrameId	frameNo;
    bool found = findFrameNo(file, pageNo, frameNo);

    // if the page to dispose is not found, do nothing
    if (!found) return;

    // the page to dispose is found // free its frame and remove its entry from hash table
    bufDescTable[frameNo].Clear();
    hashTable->remove(file, pageNo);

    // delete page from file
    file->deletePage(pageNo);
}


/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
 * Otherwise Error returned.
 *
 * @param file   	File object
* @throws  PagePinnedException If any page of the file is pinned in the buffer pool
* @throws BadBufferException If any frame allocated to the file is found to be invalid
 */
void BufMgr::flushFile(const File* file)
{
	// scan bufTable for pages belonging to the file
	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) {
            // throw BadBufferException for invalid page
            if (!bufDescTable[i].valid)
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
		    // throw PagePinnedException for pinned page
            if (bufDescTable[i].pinCnt != 0)
                throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			// if the page is dirty, flush the page to disk, set the dirty bit to false
			if (bufDescTable[i].dirty) {
				bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
				bufDescTable[i].dirty = false;
			}
			// delete page info from bufDescTable and hashTable
            hashTable->remove(file, bufDescTable[i].pageNo);
            bufDescTable[bufDescTable[i].frameNo].Clear();
		}
  	}
}


/**
* Print member variable values.
 */
void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
