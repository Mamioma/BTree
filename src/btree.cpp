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
}

template <class T>
void BTreeIndex::buildBTree(const std::string &relationName,
							BufMgr *bufMgrIn,
							const Datatype attrType,
							BlobFile* &BTreeDataFile)
{
	// allocate a new page on BTreeDataFile
	PageId BTreeID;
	Page new_page = BTreeDataFile->allocatePage(BTreeID);

	std::vector recordKey = std::vector<RIDKeyPair<T>>{};
	std::vector pageKey = std::vector<PageKeyPair<T>>{};

	// scan the file and insert key RID into vector
	// todo: here we assume it is initialized with order, but in reality, we need to sort it
	FileScan fscan(relationName, bufMgrIn);
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
					new_page.insertRecord(recordStr);

					// get the index key from record
					const char *record = recordStr.c_str();
					if (attrType == INTEGER)
					{
						int key = *((int *)(record + offsetof(RECORD, i)));
						RIDKeyPair<int> k;
						k.set(scanRid, key);
						recordKey.push_back(k);
						std::cout << key << std::endl;
					}
					else if (attrType == DOUBLE)
					{
						double key = *((double *)(record + offsetof(RECORD, d)));
						RIDKeyPair<double> k;
						k.set(scanRid, key);
						recordKey.push_back(k);
						std::cout << key << std::endl;
					}
					else if (attrType == STRING)
					{
						std::string key = (record + offsetof(RECORD, s));
						RIDKeyPair<std::string> k;
						k.set(scanRid, key);
						recordKey.push_back(k);
						std::cout << key << std::endl;
					}
				}
			}
			catch (InsufficientSpaceException &e)
			{
				
				BTreeDataFile->writePage(BTreeID, new_page);
				new_page = BTreeDataFile->allocatePage(BTreeID);
			}
		}
	}
	catch (EndOfFileException e)
	{
		std::cout << "Read all records" << std::endl;
	}

	BTreeDataFile->writePage(BTreeID, new_page);
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
		BTreeDataFile = new BlobFile(indexName, true);
		BTreeIndex::file = BTreeDataFile;
	}
	catch (const FileExistsException& e) {
		// todo: if the exsited file index doesn't match witht the inserted index, throw a BadIndexInfoException

		BTreeDataFile = new BlobFile(indexName, false);

		// here number is one because it is the metadata, and page number starts at one
		bufMgr->readPage(BTreeDataFile, 1, headerPage);

		IndexMetaInfo* metaDataInfo = (IndexMetaInfo*) headerPage;

		rootPageNum = metaDataInfo->rootPageNo;

		bufMgr->unPinPage(file, rootPageNum, false);

		return;
	}

	// copy metadata information
	IndexMetaInfo BTreeMetaData;
	memcpy(BTreeMetaData.relationName, relationName.c_str(), strlen(relationName.c_str()) + 1);
	BTreeMetaData.attrByteOffset = attrByteOffset;
	BTreeMetaData.attrType = attrType;

	// Get Records from relation file: use FileScan Class
	// plus build a BTree
	if (attrType == INTEGER) {
		buildBTree<int>(relationName, bufMgrIn, attrType, BTreeDataFile);
	} else if (attrType == DOUBLE) {
		buildBTree<double>(relationName, bufMgrIn, attrType, BTreeDataFile);
	} else if (attrType == STRING) {
		buildBTree<std::string>(relationName, bufMgrIn, attrType, BTreeDataFile);
	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

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
