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
	flushFile(bufDescTable[0].file); // 几个FILE? 可以用index 0吗？

	// deallocates the buffer pool and the BufDesc table, call destructor
	delete hashTable; // deallocate the buffer hash table
	bufPool = NULL; // ?
	bufDescTable = NULL; // ?
}

void BufMgr::advanceClock()
{
	// Advances clock to the next frame in the buffer pool.
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	// Throws BufferExceededException if all buffer frames are pinned.
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	// Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, 
	// sets the dirty bit. Throws PAGENOTPINNED if the pin count is already 0. 
	// Does nothing if page is not found in the hash table lookup.

	// use hash table lookup to find the frame number of specified page in given file
	FrameId	frameNo = NULL;
	hashTable->lookup(file, pageNo, frameNo); // address of??

	// do nothing if not found
	if (frameNo == NULL) return;

	// Throws PAGENOTPINNED if the pin count is already 0
	if (bufDescTable[frameNo].pinCnt == 0) {
		throw PageNotPinnedException(file->filename(), pageNo, frameNo);
	}
	else {
		// Decrements the pinCnt
		bufDescTable[frameNo].pinCnt--;
		if (dirty == true) {
			bufDescTable[frameNo].dirty = true;
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
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

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
