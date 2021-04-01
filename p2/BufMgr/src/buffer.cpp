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
		flushFile(bufDescTable[i].file);
  	}

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
	FrameId	frameNo = UINT32_MAX; // not sure for the initial value??
	hashTable->lookup(file, pageNo, frameNo); // address of??

	// do nothing if not found
	if (frameNo == UINT32_MAX) return;

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
	//Should scan bufTable for pages belonging to the file. 
	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) { // page belongs to the file
			// (a) if the page is dirty, call file->writePage() 
			// to flush the page to disk and then set the dirty bit for the page to false
			if (bufDescTable[i].dirty == true) {
				Page p = bufPool[bufDescTable[i].frameNo];
				file->writePage(p); // do not know why not work
				bufDescTable[i].dirty == false;
			}
			// (b) remove the page from the hashtable (whether the page is clean or dirty)
			hashTable->remove(file, bufDescTable[i].pageNo);
			// (c) invoke the Clear() method of BufDesc for the page frame.
			bufDescTable[i].Clear();

			// Throws PagePinnedException if some page of the file is pinned 
			if (bufDescTable[i].pinCnt != 0) {
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			// Throws BadBuffer- Exception if an invalid page belonging to the file is encountered
			if (bufDescTable[i].valid == false) {
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
		}
  	}
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
