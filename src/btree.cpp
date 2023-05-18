/**
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
#include "exceptions/file_exists_exception.h"
#include "exceptions/insufficient_space_exception.h"
#include <vector>

//#define DEBUG

namespace badgerdb
{

typedef struct tuple
{
	int i;
	double d;
	char s[64];
} RECORD;

const void BTreeIndex::InitializeBTreeIndex(BufMgr *bufMgrIn, 
									  		const int attrByteOffset,
									  		const Datatype attrType)
{
	BTreeIndex::bufMgr = bufMgrIn;
	BTreeIndex::attributeType = attrType;
	BTreeIndex::attrByteOffset = attrByteOffset;
	if (attrType == INTEGER)
	{
		BTreeIndex::leafOccupancy = INTARRAYLEAFSIZE;
		BTreeIndex::nodeOccupancy = INTARRAYNONLEAFSIZE;
	}
	else if (attrType == DOUBLE)
	{
		BTreeIndex::leafOccupancy = DOUBLEARRAYLEAFSIZE;
		BTreeIndex::nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
	}
	else if (attrType == STRING)
	{
		BTreeIndex::leafOccupancy = STRINGARRAYLEAFSIZE;
		BTreeIndex::nodeOccupancy = STRINGARRAYNONLEAFSIZE;
	}
	else
	{
		std::cout << "ERROR! UnSupported TYPE, we only support int, double and string";
		throw ScanNotInitializedException();
	}
	BTreeIndex::scanExecuting = false;
	BTreeIndex::headerPageNum = 1;
}

template <class T>
void BTreeIndex::buildBTree(const std::string &relationName,
							IndexMetaInfo &BTreeMetaData)
{
	// allocate a root page and a header page on BTreeDataFile
	Page* headerPage;
	Page* rootPage;

	bufMgr->allocPage(file, headerPageNum, headerPage);
	bufMgr->allocPage(file, rootPageNum, rootPage);

	// initalize the size of the root to zeros
	if (attributeType == INTEGER) {
		((LeafNodeInt*) rootPage)->size = 0;
	} else if (attributeType == DOUBLE) {
		((LeafNodeDouble*) rootPage)->size = 0;
	} else {
		((LeafNodeString*) rootPage)->size = 0;
	}

	// save it in the metadata file
	BTreeMetaData.rootPageNo = rootPageNum;
	BTreeMetaData.isLeafPage = true;

	// copy the metadata into header page
	memcpy(headerPage, &BTreeMetaData, sizeof(IndexMetaInfo));

	// scan the file and insert key RID into vector
	// todo: here we assume it is initialized with order, but in reality, we need to sort it
	FileScan fscan(relationName, bufMgr);
	try
	{
		while (1) {
			RecordId scanRid;
			try
			{
				while (1)
				{
					fscan.scanNext(scanRid);

					std::string recordStr = fscan.getRecord();
					// new_page.insertRecord(recordStr);

					// get the index key from record
					const char *record = recordStr.c_str();
					if (BTreeMetaData.attrType == INTEGER)
					{
						int key = *((int *)(record + offsetof(RECORD, i)));
						insertEntry(&key, scanRid);
					}
					else if (BTreeMetaData.attrType == DOUBLE)
					{
						double key = *((double *)(record + offsetof(RECORD, d)));
						insertEntry(&key, scanRid);
					}
					else if (BTreeMetaData.attrType == STRING)
					{
						std::string key = (record + offsetof(RECORD, s));
						insertEntry(&key, scanRid);
					}
				}
			}
			catch (InsufficientSpaceException &e)
			{
				// todo: allocate a new page 
				// BTreeDataFile->writePage(BTreeID, new_page);
				// new_page = BTreeDataFile->allocatePage(BTreeID);
			}
		}
	}
	catch (EndOfFileException e)
	{
		std::cout << "Read all records" << std::endl;
		// data is written into it, set dirty bit to true
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);
	}

	// BTreeDataFile->writePage(BTreeID, new_page);
}

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	// initialize the variable in struct
	InitializeBTreeIndex(bufMgrIn, attrByteOffset, attrType);

	// first construct the indexfile by concatenating the relation name with the offset of the attribute over which the index is built
	std::ostringstream idxStr;
	idxStr << relationName << ' . ' << attrByteOffset;
	std ::string indexName = idxStr.str();
	std::cout << indexName << std::endl;
	outIndexName = indexName;

	// create a B+ Tree BlobFile to store the index
	// if a B+ Tree with the indexName already been created, catch a FileExistsException
	BlobFile* BTreeDataFile;
	Page* headerPage;
	Page* rootPage;
	try {
		file = new BlobFile(indexName, true);
	}
	catch (const FileExistsException& e) {
		// todo: if the exsited file index doesn't match witht the inserted index, throw a BadIndexInfoException

		file = new BlobFile(indexName, false);

		// here number is one because it is the metadata, and page number starts at one
		bufMgr->readPage(file, headerPageNum, headerPage);

		IndexMetaInfo* metaDataInfo = (IndexMetaInfo*) headerPage;

		rootPageNum = metaDataInfo->rootPageNo;

		bufMgr->unPinPage(file, headerPageNum, false);

		return;
	}

	// copy metadata information
	IndexMetaInfo BTreeMetaData;
	memcpy(BTreeMetaData.relationName, relationName.c_str(), STRINGSIZE);
	BTreeMetaData.attrByteOffset = attrByteOffset;
	BTreeMetaData.attrType = attrType;

	// Get Records from relation file: use FileScan Class
	// plus build a BTree
	if (attrType == INTEGER) {
		buildBTree<int>(relationName, BTreeMetaData);
	} else if (attrType == DOUBLE) {
		buildBTree<double>(relationName, BTreeMetaData);
	} else if (attrType == STRING) {
		buildBTree<std::string>(relationName, BTreeMetaData);
	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	try {
		bufMgr->flushFile(file);
	}
	catch (const std::exception &e)
	{
		std::cout << "ERROR when flushing the file" << std::endl;
	}
	
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
template <class T, class LeafType, class NonLeafType>
void BTreeIndex::splitLeafPage(PageId &rootPageNum, const void *key, const RecordId rid)
{
	// read the leaf page that need to be splited
	Page* currentRootPage;
	bufMgr->readPage(file, rootPageNum, currentRootPage);
	bufMgr->unPinPage(file, rootPageNum, false);
	// here pin twice because in BTreeIndex::insertEntry(), I read rootPage without pin, so I pin here
	bufMgr->unPinPage(file, rootPageNum, false);
	// cast it into leaf struct
	LeafType *leafNodeLeft = reinterpret_cast<LeafType *>(currentRootPage);

	// allocate a new root page
	// unpin the page and set dirty bit to true, because we will modify it later
	Page* newRootPage;
	PageId newRootId;
	bufMgr->allocPage(file, newRootId, newRootPage);
	bufMgr->unPinPage(file, newRootId, true);

	// read the metaData page
	Page* headerPage;
	bufMgr->readPage(file, headerPageNum, headerPage);
	bufMgr->unPinPage(file, headerPageNum, true);
	IndexMetaInfo* metaData = (IndexMetaInfo*) headerPage;
	// update the metaData page
	metaData->isLeafPage = false;
	metaData->rootPageNo = newRootId;

	/**
	 * @brief point non leaf node to sibling
	 */
	NonLeafType *nonLeafNode = reinterpret_cast<NonLeafType* >(newRootPage);
	// set level to 1 because it is just above leaf node
	nonLeafNode->level = 1;
	nonLeafNode->size = 1;
	// push the right sibling's first record's key value up the non leaf node
	nonLeafNode->keyArray[0] = *(T*) key;
	// set the nonLeafNode's pointer, pointing to left sibling
	nonLeafNode->pageNoArray[0] = rootPageNum;
	// create a new leaf page for storing the new data
	PageId newLeafNodeId;
	Page* newLeafNode;
	bufMgr->allocPage(file, newLeafNodeId, newLeafNode);
	bufMgr->unPinPage(file, newLeafNodeId, true);
	LeafType *leafNodeRight = reinterpret_cast<LeafType*>(newLeafNode);
	// set the nonLeafNode's pointer, pointing to right sibling
	nonLeafNode->pageNoArray[1] = newLeafNodeId;

	/**
	 * @brief set metaData and sibling
	 */
	// insert key into right sibling node
	leafNodeRight->size = 1;
	leafNodeRight->keyArray[0] = *(T*) key;
	leafNodeRight->ridArray[0] = rid;
	// point left sibling to right sibling
	leafNodeLeft->rightSibPageNo = newLeafNodeId;
	// update rootPageNum
	BTreeIndex::rootPageNum = newRootId;
}

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// read the header page and cast it into IndexMetaInfo
	Page* headerPage;
	bufMgr->readPage(file, headerPageNum, headerPage);
	bufMgr->unPinPage(file, headerPageNum, false);
	IndexMetaInfo* metaDataPage = (IndexMetaInfo*) headerPage;

	// if it a leaf page, which indicates that there is only one level 
	// in other words, there are no nonLeafPage.
	if (metaDataPage->isLeafPage) 
	{
		// read the root page
		Page *rootPage;
		bufMgr->readPage(file, rootPageNum, rootPage);
		
		// insert the data if the size doesn't excced the limit
		if (attributeType == INTEGER) 
		{
			if (((LeafNodeInt *)rootPage)->size < INTARRAYLEAFSIZE) 
			{
				// insert data into root page 
				LeafNodeInt* leafInt = reinterpret_cast<LeafNodeInt*>(rootPage);
				leafInt->keyArray[leafInt->size] = *(int*)key;
				std::cout << *(int *)key << std::endl;
				leafInt->ridArray[leafInt->size] = rid;
				leafInt->size++;

				// unpin page and set the dirty bit to true, because the data is modified
				bufMgr->unPinPage(file, rootPageNum, true);
			} else 
			{
				// handle the situation that size excced the limit
				splitLeafPage<int, LeafNodeInt, NonLeafNodeInt>(rootPageNum, key, rid);
			}
		} else if (attributeType == DOUBLE) 
		{
			if (((LeafNodeDouble *)rootPage)->size < DOUBLEARRAYLEAFSIZE)
			{
				// insert data into root page
				LeafNodeDouble *leafDouble = reinterpret_cast<LeafNodeDouble *>(rootPage);
				leafDouble->keyArray[leafDouble->size] = *(double *)key;
				std::cout << leafDouble->keyArray[leafDouble->size] << std::endl;
				leafDouble->ridArray[leafDouble->size] = rid;
				leafDouble->size++;

				// unpin page and set the dirty bit to true, because the data is modified
				bufMgr->unPinPage(file, rootPageNum, true);
			} else 
			{
				// handle the situation that size excced the limit
				splitLeafPage<double, LeafNodeDouble, NonLeafNodeDouble>(rootPageNum, key, rid);
			}
		} else 
		{
			if (((LeafNodeString *)rootPage)->size < STRINGARRAYLEAFSIZE)
			{
				// insert data into root page
				LeafNodeString *leafString = reinterpret_cast<LeafNodeString *>(rootPage);
				for (int i = 0; i < STRINGSIZE; i++)
				{
					leafString->keyArray[leafString->size][i] = (*(std::string *)key)[i];
				}
				std::cout << strlen(leafString->keyArray[leafString->size]) << " ";
				std::cout << leafString->keyArray[leafString->size] << std::endl;
				leafString->ridArray[leafString->size] = rid;
				leafString->size++;

				// unpin page and set the dirty bit to true, because the data is modified
				bufMgr->unPinPage(file, rootPageNum, true);
			}
			else
			{
				// handle the situation that size excced the limit
				splitLeafPage<std::string, LeafNodeString, NonLeafNodeString>(rootPageNum, key, rid);
			}
		}
	} else 
	{
		// todo: dealing with non leaf pages
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
