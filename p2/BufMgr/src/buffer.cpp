/**
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

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

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


BufMgr::~BufMgr() 
{
	// Flushes out all dirty pages
	for (FrameId i = 0; i < numBufs; i++) {
        bufDescTable[i].pinCnt = 0;
		if (bufDescTable[i].valid && bufDescTable[i].dirty) {
            bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
            bufDescTable[i].dirty = false;
        }
  	}

	// deallocates the buffer pool and the BufDesc table, call destructor
	delete hashTable; // deallocate the buffer hash table
	bufPool = NULL; // todo: check whether implementation is correct
	bufDescTable = NULL; // todo: check whether implementation is correct
}

void BufMgr::advanceClock()
{
	// Advances clock to the next frame in the buffer pool
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
    advanceClock();
    FrameId startID = clockHand; // start frame
    uint32_t numPinned = 0; // number of frames that are pinned
    while(true){
        if (bufDescTable[clockHand].valid){
            // valid is set
            // increase pin count if page is pinned
            if(bufDescTable[clockHand].pinCnt > 0)
                numPinned++;
            if (bufDescTable[clockHand].refbit){
                // refbit is set, clear the refbit and continue.
                bufDescTable[clockHand].refbit = false;
                advanceClock();
                continue;
            }
            else{
                if (bufDescTable[clockHand].pinCnt > 0){
                    // refbit is not set but frame is pinned.
                    if(numPinned == numBufs + 1 && startID == clockHand)
                        throw BufferExceededException(); // all frames are pinned
                     else{
                         advanceClock();
                         continue;
                     }
                }
                else{
                    // refbit is not set and the frame is not pinned, use this frame.
                    if(bufDescTable[clockHand].dirty){
                        // dirty page is set, flush page to disk
                        bufDescTable[clockHand].file->writePage(bufPool[bufDescTable[clockHand].frameNo]);
                        bufDescTable[clockHand].dirty = false;
                    }
                    // remove the page from the hashtable
                    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                    // invoke the Clear() method of BufDesc for the page frame.
                    bufDescTable[clockHand].Clear();
                    frame = clockHand;
                    return;
                }
            }
        } else{
            // valid is not set for the current frame, use this frame
            frame = clockHand;
            return;
        }
    }
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	bool found = true; // if the page is found
	FrameId	frameNo;
	try {
		hashTable->lookup(file, pageNo, frameNo);
	}
	catch (const HashNotFoundException &e) {
		found = false; // throw exception when page is not in the buffer pool
	}
	
	// if not found
	if (!found) {
		// Call allocBuf() to allocate a buffer frame 
		allocBuf(frameNo);
        Page page_obj = file->readPage(pageNo); // if page does not exists, an exception will be thrown
		// no exception thrown, read the page from disk into the buffer pool frame
        hashTable->insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file, pageNo);
        bufPool[frameNo] = page_obj;
        page = bufPool + frameNo;
	}
	else {
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		// return a pointer to the frame containing the page
		page = bufPool + frameNo;
	}
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	// use hash table lookup to find the frame number of specified page in given file
	bool found = true; // if the page is found
	FrameId	frameNo;
	try {
		hashTable->lookup(file, pageNo, frameNo); // address of??
	}
	catch (const HashNotFoundException &e) {
		found = false; // throw exception when page is not in the buffer pool
	}

	// do nothing if not found
	if (!found) return;

	// Throws PAGENOTPINNEDException if the pin count is already 0
	if (bufDescTable[frameNo].pinCnt == 0) {
		throw PageNotPinnedException(file->filename(), pageNo, frameNo);
	}
	else {
		// Decrements the pinCnt
		bufDescTable[frameNo].pinCnt--;
		if (dirty) {
			bufDescTable[frameNo].dirty = true;
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	// AllocBuf() is called to obtain a buffer pool frame.
	FrameId	frameNo;
	allocBuf(frameNo);

	// Assign new page into that newly allocated page frame
    bufPool[frameNo] = file->allocatePage();
    page = &bufPool[frameNo];
    pageNo = page->page_number();

	// entry inserted into the hash table and Set() the frame
	hashTable->insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);
}

void BufMgr::flushFile(const File* file)
{
	// Should scan bufTable for pages belonging to the file.
	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) { // page belongs to the file
            // Throws BadBufferException if an invalid page
            if (!bufDescTable[i].valid) {
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }
		    // Throws PagePinnedException if some page of the file is pinned
            if (bufDescTable[i].pinCnt != 0) {
                throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
            }
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

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	// use hash table lookup to find the frame number of specified page in given file
	bool found = true;
	FrameId	frameNo;
	try {
		hashTable->lookup(file, PageNo, frameNo); // address of??
	}
	catch (const HashNotFoundException &e) {
		found = false;
	}

	// todo: don't need to delete page if not found?
	// do nothing if not found
	if (!found) return;

	// free frame and remove entry from hash table.
	bufDescTable[frameNo].Clear();
	hashTable->remove(file, PageNo);

	// todo: what if the page is dirty? don't need to flush it before deletion?
	// delete page from file
    file->deletePage(PageNo);
}

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
