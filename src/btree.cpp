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
	BTreeIndex::fullTime = 0;
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
void BTreeIndex::insertDataLeaf(PageId pageId, int position, const void *key)
{
	Page* leafPage;
	bufMgr->readPage(file, pageId, leafPage);
	bufMgr->unPinPage(file, pageId, true);
	if (attributeType == INTEGER) 
	{
		LeafNodeInt* leafNode = reinterpret_cast<LeafNodeInt*>(leafPage);
		leafNode->keyArray[position] = *(int*) key;
	} else if (attributeType == DOUBLE) 
	{
		LeafNodeDouble* leafNode = reinterpret_cast<LeafNodeDouble*>(leafPage);
		leafNode->keyArray[position] = *(double*) key;
	} else 
	{
		LeafNodeString* leafNode = reinterpret_cast<LeafNodeString*>(leafPage);
		for (int i = 0; i < STRINGSIZE; i++)
		{
			leafNode->keyArray[position][i] = (*(std::string *)key)[i];
		}
	}
}

void BTreeIndex::insertDataNonLeaf(PageId pageId, int position, const void *key)
{
	Page *nonLeafPage;
	bufMgr->readPage(file, pageId, nonLeafPage);
	bufMgr->unPinPage(file, pageId, true);
	if (attributeType == INTEGER)
	{
		NonLeafNodeInt *nonLeafNode = reinterpret_cast<NonLeafNodeInt *>(nonLeafPage);
		nonLeafNode->keyArray[position] = *(int*) key;
		std::cout << "non leaf key: " << nonLeafNode->keyArray[position] << " position: " << position << std::endl;
	}
	else if (attributeType == DOUBLE)
	{
		NonLeafNodeDouble *nonLeafNode = reinterpret_cast<NonLeafNodeDouble *>(nonLeafPage);
		nonLeafNode->keyArray[position] = *(double *)key;
	}
	else
	{
		NonLeafNodeString *nonLeafNode = reinterpret_cast<NonLeafNodeString *>(nonLeafPage);
		for (int i = 0; i < STRINGSIZE; i++)
		{
			nonLeafNode->keyArray[position][i] = (*(std::string *)key)[i];
		}
		std::cout << "non leaf key: " << nonLeafNode->keyArray[position] << " position: " << position << std::endl;
	}
}

template <class Type1, class Type2>
void BTreeIndex::insertDataAnyTypeString(PageId pageId, int position, PageId keyPageId, int keyPagePosition)
{
	Page *nonLeafPage;
	bufMgr->readPage(file, pageId, nonLeafPage);
	bufMgr->unPinPage(file, pageId, true);
	Type1 *nonLeafNode = reinterpret_cast<Type1 *>(nonLeafPage);

	Page *nonLeafKeyPage;
	bufMgr->readPage(file, keyPageId, nonLeafKeyPage);
	bufMgr->unPinPage(file, keyPageId, false);
	Type2 *nonLeafKeyNode = reinterpret_cast<Type2 *>(nonLeafKeyPage);
	std::cout << "--------key: " << nonLeafKeyNode->keyArray[keyPagePosition] << std::endl;
	for (int i = 0; i < STRINGSIZE; i++)
	{
		nonLeafNode->keyArray[position][i] = nonLeafKeyNode->keyArray[keyPagePosition][i];
	}
	std::cout << "non leaf key: " << nonLeafNode->keyArray[position] << " position: " << position << std::endl;
}

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
	insertDataNonLeaf(newRootId, 0, key);
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
	insertDataLeaf(newLeafNodeId, 0, key);
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
				leafInt->ridArray[leafInt->size] = rid;
				leafInt->size++;

				// unpin page and set the dirty bit to true, because the data is modified
				bufMgr->unPinPage(file, rootPageNum, true);
			} else 
			{
				// handle the situation that size excced the limit
				std::cout << "handle the situation that size excced the limit" << std::endl;
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
				leafString->ridArray[leafString->size] = rid;
				leafString->size++;

				// unpin page and set the dirty bit to true, because the data is modified
				bufMgr->unPinPage(file, rootPageNum, true);
			}
			else
			{
				// handle the situation that size excced the limit
				std::cout << "handle the situation that size excced the limit" << std::endl;
				splitLeafPage<std::string, LeafNodeString, NonLeafNodeString>(rootPageNum, key, rid);
			}
		}
	} else 
	{
		// todo: dealing with non leaf pages
		if (attributeType == INTEGER) 
		{
			traverseNode<int, LeafNodeInt, NonLeafNodeInt>(rootPageNum, key, rid);
		} else if (attributeType == DOUBLE) 
		{
			traverseNode<double, LeafNodeDouble, NonLeafNodeDouble>(rootPageNum, key, rid);
		} else 
		{
			traverseNode<std::string, LeafNodeString, NonLeafNodeString>(rootPageNum, key, rid);
		}
	}

}

bool BTreeIndex::compareNonLeafKey(PageId pageId, int index, const void* key)
{
	if (attributeType == INTEGER)
	{
		Page* nonLeafPage;
		bufMgr->readPage(file, pageId, nonLeafPage);
		bufMgr->unPinPage(file, pageId, false);
		NonLeafNodeInt* nonLeafNode = reinterpret_cast<NonLeafNodeInt* >(nonLeafPage);
		return nonLeafNode->keyArray[index] <= *(int*)key;
	} else if (attributeType == DOUBLE)
	{
		Page *nonLeafPage;
		bufMgr->readPage(file, pageId, nonLeafPage);
		bufMgr->unPinPage(file, pageId, false);
		NonLeafNodeDouble *nonLeafNode = reinterpret_cast<NonLeafNodeDouble *>(nonLeafPage);
		return nonLeafNode->keyArray[index] <= *(double *)key;
	} else
	{
		Page *nonLeafPage;
		bufMgr->readPage(file, pageId, nonLeafPage);
		bufMgr->unPinPage(file, pageId, false);
		NonLeafNodeString *nonLeafNode = reinterpret_cast<NonLeafNodeString *>(nonLeafPage);
		int res = strcmp(nonLeafNode->keyArray[index], (*(std::string *)(key)).c_str());
		if (res <= 0)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}

bool BTreeIndex::compareKey(void* nodeKey, const void* key)
{
	if (attributeType == INTEGER)
	{
		return *(int*)nodeKey <= *(int*)key;
	} else if (attributeType == DOUBLE)
	{
		return *(double*)nodeKey <= *(double*)key;
	} else 
	{
		std::cout << "-----------------";
		std::cout << *(std::string*) key << "----------------" << nodeKey << std::endl;
		int res = strcmp((char*)(nodeKey), (char *)key);
		if (res <= 0)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}

template <class T, class LeafType, class NonLeafType>
void BTreeIndex::splitLeafPageAndInsertEntry(PageId &rootPageNum, PageId leafPageId, const void *key, RecordId rid)
{
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);
	NonLeafType* rootNode = reinterpret_cast<NonLeafType* >(rootPage);

	Page* leafPage;
	bufMgr->readPage(file, leafPageId, leafPage);
	bufMgr->unPinPage(file, leafPageId, false);
	LeafType* leafNode = reinterpret_cast<LeafType* >(leafPage);
	if (!whetherLeafIsFull(leafNode->size)) 
	{
		throw BadIndexInfoException("the leaf node has empty space");
	}

	Page* newLeafPage;
	PageId newLeafPageId;
	bufMgr->allocPage(file, newLeafPageId, newLeafPage);
	bufMgr->unPinPage(file, newLeafPageId, true);
	LeafType* newLeafNode = reinterpret_cast<LeafType* >(newLeafPage);

	insertDataLeaf(newLeafPageId, 0, key);
	newLeafNode->size = 1;
	newLeafNode->ridArray[0] = rid;
	leafNode->rightSibPageNo = newLeafPageId;

	// if rootPage is not full, then just update the pointer
	if (!whetherNonLeafIsFull(rootPageNum))
	{
		insertDataNonLeaf(rootPageNum, rootNode->size, key);
		rootNode->size++;
		rootNode->pageNoArray[rootNode->size] = newLeafPageId;
		bufMgr->unPinPage(file, rootPageNum, true);
	} else 
	{
		bufMgr->unPinPage(file, rootPageNum, false);
		return;
	}
}

template <class T, class LeafType, class NonLeafType>
void BTreeIndex::traverseNode(PageId &rootPageNum, const void *key, RecordId rid)
{
	Page *rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);
	NonLeafType *nonLeafNode = reinterpret_cast<NonLeafType *>(rootPage);

	// if it's just above the leaf Node, then check where to insert the entry
	if (nonLeafNode->level == 1) 
	{
		// variable index: know the location pointing to the leaf Node
		int index = 0;
		for (index = 0; index < nonLeafNode->size; index++)
		{
			if (compareNonLeafKey(rootPageNum, index, key))
			{
				// if the key is greater than the key in the leaf, continue searching
				continue;
			}
			else
			{
				// if not, break
				break;
			}
		}
		// go to leaf node according to index:
		Page* leafPage;
		PageId leafPageId = nonLeafNode->pageNoArray[index];
		bufMgr->readPage(file, leafPageId, leafPage);
		LeafType* leafNode = reinterpret_cast<LeafType* >(leafPage);
		std::cout << "index: " << index << " key: " << leafNode->keyArray[leafNode->size - 1] << " current key: " << *(T *)key << std::endl;
		// check if the page is filled
		if (!whetherLeafIsFull(leafNode->size))
		{
			// the leaf node has space, insert key and record
			insertDataLeaf(leafPageId, leafNode->size, key);
			leafNode->ridArray[leafNode->size] = rid; 
			leafNode->size++;
			bufMgr->unPinPage(file, leafPageId, true);
			bufMgr->unPinPage(file, rootPageNum, false);
		}
		else {
			// split page when leaf Node is full
			std::cout << "leaf node is filled, key is: " << *(T*)key << std::endl;
			bufMgr->unPinPage(file, leafPageId, false);
			splitLeafPageAndInsertEntry<T, LeafType, NonLeafType>(rootPageNum, leafPageId, key, rid);
			bufMgr->unPinPage(file, rootPageNum, true);
			// if the node is not the global root node, it can be done recursivly
			// if so, then we need to create a new global root and update the meta data
			if (whetherNonLeafIsFull(rootPageNum) && rootPageNum == BTreeIndex::rootPageNum) 
			{
				std::cout << "root node is filled, size is:" << nonLeafNode->size << " key is: " << *(T *)key << std::endl;
				Page* newLeafPage;
				PageId newLeafPageId = nonLeafNode->pageNoArray[nonLeafNode->size];
				bufMgr->readPage(file, newLeafPageId, newLeafPage);
				bufMgr->unPinPage(file, newLeafPageId, false);
				LeafType* newLeafNode = reinterpret_cast<LeafType* >(newLeafPage);
				std::cout << "size: " << newLeafNode->size << " key: " << newLeafNode->keyArray[newLeafNode->size - 1] << std::endl;
				if (!whetherLeafIsFull(newLeafNode->size))
				{
					throw BadIndexInfoException("the leaf Node should be filled");
				}

				Page* newLeafRightSiblingPage;
				PageId newLeafRightSiblingPageId = newLeafNode->rightSibPageNo;
				bufMgr->readPage(file, newLeafRightSiblingPageId, newLeafRightSiblingPage);
				bufMgr->unPinPage(file, newLeafRightSiblingPageId, false);
				LeafType *newLeafRightSiblingNode = reinterpret_cast<LeafType *>(newLeafRightSiblingPage);
				if (newLeafRightSiblingNode->size != 1) 
				{
					throw BadIndexInfoException("the size of the node should be 1");
				}

				Page* rootRightSiblingPage;
				PageId rootRightSiblingPageId;
				bufMgr->allocPage(file, rootRightSiblingPageId, rootRightSiblingPage);
				bufMgr->unPinPage(file, rootRightSiblingPageId, true);
				NonLeafType *nonLeafRightSiblingNode = reinterpret_cast<NonLeafType *>(rootRightSiblingPage);
				nonLeafRightSiblingNode->level = 1;
				nonLeafRightSiblingNode->size = 0;
				if (attributeType == STRING) 
				{
					insertDataAnyTypeString<NonLeafNodeString, LeafNodeString>(rootRightSiblingPageId, 0, newLeafRightSiblingPageId, 0);
				} else 
				{
					insertDataNonLeaf(rootRightSiblingPageId, 0, &newLeafRightSiblingNode->keyArray[0]);
				}
				nonLeafRightSiblingNode->pageNoArray[0] = newLeafPageId;
				nonLeafRightSiblingNode->size++;
				nonLeafRightSiblingNode->pageNoArray[1] = newLeafRightSiblingPageId;
				// remeber to subtract the size of current root node
				nonLeafNode->size--;

				Page* globalRootPage;
				PageId globalRootPageId;
				bufMgr->allocPage(file, globalRootPageId, globalRootPage);
				bufMgr->unPinPage(file, globalRootPageId, true);
				NonLeafType *globalRootNode = reinterpret_cast<NonLeafType *>(globalRootPage);
				globalRootNode->level = 0;
				globalRootNode->size = 0;
				if (attributeType == STRING)
				{
					insertDataAnyTypeString<NonLeafNodeString, NonLeafNodeString>(globalRootPageId, 0, rootRightSiblingPageId, 0);
				} else 
				{
					insertDataNonLeaf(globalRootPageId, 0, &nonLeafRightSiblingNode->keyArray[0]);
				}
				globalRootNode->pageNoArray[0] = rootPageNum;
				globalRootNode->size++;
				globalRootNode->pageNoArray[1] = rootRightSiblingPageId;

				// update the metaData
				Page* headerPage;
				PageId headerPageId = BTreeIndex::headerPageNum;
				if (headerPageId != 1)
				{
					throw BadIndexInfoException("the header page id shoule be 1");
				}
				bufMgr->readPage(file, headerPageId, headerPage);
				bufMgr->unPinPage(file, headerPageId, true);
				IndexMetaInfo* metaData = (IndexMetaInfo*) headerPage;
				metaData->rootPageNo = globalRootPageId;
				BTreeIndex::rootPageNum = globalRootPageId;
			}
		}
	} else 
	{
		std::cout << "traverse start key: " << nonLeafNode->keyArray[nonLeafNode->size-1];
		int index = 0;
		for (index = 0; index < nonLeafNode->size; index++) 
		{
			if (compareNonLeafKey(rootPageNum, index, key)) 
			{
				// if the key is greater than the key in the leaf, continue searching
				continue;
			}
			else
			{
				// if not, break
				break;
			}
		}
		PageId nextLevelPageId = nonLeafNode->pageNoArray[index];
		std::cout << "  index: " << index << " page number: " << nextLevelPageId << " root page number: " << rootPageNum << std::endl;
		traverseNode<T, LeafType, NonLeafType>(nextLevelPageId, key, rid);

		Page* NonLeafPage;
		bufMgr->readPage(file, nextLevelPageId, NonLeafPage);
		NonLeafType* NextLevelNonLeafNode = reinterpret_cast<NonLeafType* >(NonLeafPage);
		if (whetherNonLeafIsFull(nextLevelPageId))
		{
			fullTime += 1;
			if (fullTime % 2 == 1) {
				return;
			}
			// here dirty bit is set to true because later we will subtract size and assign
			// the node to its right sibling
			bufMgr->unPinPage(file, nextLevelPageId, true);
			
			// find the Id
			PageId targetId = NextLevelNonLeafNode->pageNoArray[NextLevelNonLeafNode->size];
			Page* searchPage;
			bufMgr->readPage(file, targetId, searchPage);
			bufMgr->unPinPage(file, targetId, false);
			// we assume the height is 3, otherwise we need a hashmap <pageId, type of node>
			// to see if it is a leaf node or a non leaf node
			LeafType* newLeafNode = reinterpret_cast<LeafType* >(searchPage);
			if (!whetherLeafIsFull(newLeafNode->size))
			{
				throw BadIndexInfoException("the leaf Node should be filled");
			}
			PageId IdWeWant = newLeafNode->rightSibPageNo;

			// Read the Page and find the key
			Page* PageWeWant;
			bufMgr->readPage(file, IdWeWant, PageWeWant);
			bufMgr->unPinPage(file, IdWeWant, false);
			LeafType* NodeWeWant = reinterpret_cast<LeafType* >(PageWeWant);
			if (NodeWeWant->size != 1) 
			{
				throw BadIndexInfoException("the size of the node should be 1");
			}
			Page *newNonLeafPage;
			PageId newNonLeafPageId;
			bufMgr->allocPage(file, newNonLeafPageId, newNonLeafPage);
			bufMgr->unPinPage(file, newNonLeafPageId, true);
			NonLeafType *newNonLeafNode = reinterpret_cast<NonLeafType *>(newNonLeafPage);
			newNonLeafNode->level = 1;
			newNonLeafNode->size = 0;
			// insert key
			if (attributeType == STRING)
			{
				insertDataAnyTypeString<NonLeafNodeString, LeafNodeString>(newNonLeafPageId, newNonLeafNode->size, IdWeWant, 0);
			} else 
			{
				insertDataNonLeaf(newNonLeafPageId, newNonLeafNode->size, &NodeWeWant->keyArray[0]);
			}
			newNonLeafNode->pageNoArray[newNonLeafNode->size] = targetId;
			newNonLeafNode->size++;
			newNonLeafNode->pageNoArray[newNonLeafNode->size] = IdWeWant;

			// update root node
			if (whetherNonLeafIsFull(rootPageNum)) 
			{
				throw BadIndexInfoException("the height of the B+ Tree is over 3, need more complicated implementation");
			}
			/**
			 * @brief here need to subtract size because we have assign the right most to another node
			 */
			NextLevelNonLeafNode->size--;
			if (attributeType == STRING)
			{
				insertDataAnyTypeString<NonLeafNodeString, NonLeafNodeString>(rootPageNum, nonLeafNode->size, newNonLeafPageId, newNonLeafNode->size - 1);
			} else 
			{
				insertDataNonLeaf(rootPageNum, nonLeafNode->size, &newNonLeafNode->keyArray[newNonLeafNode->size - 1]);
			}
			nonLeafNode->size++;
			nonLeafNode->pageNoArray[nonLeafNode->size] = newNonLeafPageId;
			bufMgr->unPinPage(file, rootPageNum, true);
		} else 
		{
			bufMgr->unPinPage(file, nextLevelPageId, false);
		}
	}
}

bool BTreeIndex::whetherLeafIsFull(int size) 
{
	if (attributeType == INTEGER) 
	{
		if (size < INTARRAYLEAFSIZE) 
		{
			return false;
		} else 
		{
			return true;
		}
	} else if (attributeType == DOUBLE) 
	{
		if (size < DOUBLEARRAYLEAFSIZE)
		{
			return false;
		} else 
		{
			return true;
		}
	} else 
	{
		if (size < STRINGARRAYLEAFSIZE) 
		{
			return false;
		} else 
		{
			return true;
		}
	}
}

bool BTreeIndex::whetherNonLeafIsFull(PageId pageId) 
{
	Page* nonLeafPage;
	bufMgr->readPage(file, pageId, nonLeafPage);
	bufMgr->unPinPage(file, pageId, false);
	if (attributeType == INTEGER)
	{
		NonLeafNodeInt* NonLeafNode = reinterpret_cast<NonLeafNodeInt* >(nonLeafPage);
		int size = NonLeafNode->size;
		if (size < INTARRAYNONLEAFSIZE)
		{
			return false;
		}
		else
		{
			Page* leafPage;
			PageId leafPageId = NonLeafNode->pageNoArray[NonLeafNode->size];
			bufMgr->readPage(file, leafPageId, leafPage);
			bufMgr->unPinPage(file, leafPageId, false);
			LeafNodeInt* LeafNode = reinterpret_cast<LeafNodeInt* >(leafPage);
			if (LeafNode->size < INTARRAYLEAFSIZE) 
			{
				return false;
			}
			else 
			{
				return true;
			}
		}
	}
	else if (attributeType == DOUBLE)
	{
		NonLeafNodeDouble *NonLeafNode = reinterpret_cast<NonLeafNodeDouble *>(nonLeafPage);
		int size = NonLeafNode->size;
		if (size < DOUBLEARRAYNONLEAFSIZE)
		{
			return false;
		}
		else
		{
			Page *leafPage;
			PageId leafPageId = NonLeafNode->pageNoArray[NonLeafNode->size];
			bufMgr->readPage(file, leafPageId, leafPage);
			bufMgr->unPinPage(file, leafPageId, false);
			LeafNodeDouble *LeafNode = reinterpret_cast<LeafNodeDouble *>(leafPage);
			if (LeafNode->size < DOUBLEARRAYLEAFSIZE)
			{
				return false;
			}
			else
			{
				return true;
			}
		}
	}
	else
	{
		NonLeafNodeString *NonLeafNode = reinterpret_cast<NonLeafNodeString *>(nonLeafPage);
		int size = NonLeafNode->size;
		if (size < STRINGARRAYNONLEAFSIZE)
		{
			return false;
		}
		else
		{
			Page *leafPage;
			PageId leafPageId = NonLeafNode->pageNoArray[NonLeafNode->size];
			bufMgr->readPage(file, leafPageId, leafPage);
			bufMgr->unPinPage(file, leafPageId, false);
			LeafNodeString *LeafNode = reinterpret_cast<LeafNodeString *>(leafPage);
			if (LeafNode->size < STRINGARRAYLEAFSIZE)
			{
				return false;
			}
			else
			{
				return true;
			}
		}
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
