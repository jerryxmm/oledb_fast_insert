#include "StdAfx.h"
#include "SimDB_ado.h"

//#include "msado15.tlh"
#include <time.h>
#include <iostream>
#define DB_TIMEOUT 10000
#define PROCESS_ERROR(e) Proc_Error(e, __LINE__)
#define COLUMN_ALIGNVAL_8 8 //程序结构体为8字节对齐方式，#define COLUMN_ALIGNVAL 8
#define COLUMN_ALIGNVAL_4 4 //程序结构体为4字节对齐方式，#define COLUMN_ALIGNVAL 4
#define ROUND_UP(Size, Amount)(((DWORD)(Size) + ((Amount)-1)) & ~((Amount)-1))

inline string _v2s(const _variant_t& t)
{
	if (t.vt == VT_NULL || t.vt == VT_EMPTY)
	{
		return string();
	}
	return string(_bstr_t(t.bstrVal));
}

inline long _v2i(const _variant_t& t)
{
	if (t.vt == VT_NULL || t.vt == VT_EMPTY)
	{
		return 0;
	}
	return t.lVal;
}

inline double _v2f(const _variant_t& t)
{
	if (t.vt == VT_NULL || t.vt == VT_EMPTY)
	{
		return 0.F;
	}
	return t.dblVal;
}

enum ADO_EVENT_TYPE
{
	ADOEV_INFOMESSAGE = 1,
	ADOEV_BEGINTRANSCOMPLETE = 2,
	ADOEV_COMMITTRANSCOMPLETE = 3,
	ADOEV_ROLLBACKTRANSCOMPLETE = 4,
	ADOEV_WILLEXECUTE = 5,
	ADOEV_EXECCOMPLETE = 6,
	ADOEV_WILLCONNECT = 7,
	ADOEV_CONNECTCOMPLETE = 8,
	ADOEV_DISCONNECT = 9
};
typedef  void(*ado_callback)(void* owner, int ev, void* data);

CSimDB_ADO::CSimDB_ADO()
{
	m_pIFastLoad = NULL;
	m_hAccessor = 0;
	m_pIDBInitialize = NULL;
	m_pIDBCreateSession = NULL;
	m_pIMalloc = NULL;
}

CSimDB_ADO::~CSimDB_ADO()
{

}
// DumpErrorInfo queries SQLOLEDB error interfaces, retrieving available
// status or error information.
void CSimDB_ADO::DumpErrorInfo(IUnknown* pObjectWithError, REFIID IID_InterfaceWithError)
{
	// Interfaces used in the example.
	IErrorInfo*             pIErrorInfoAll = NULL;
	IErrorInfo*             pIErrorInfoRecord = NULL;
	IErrorRecords*          pIErrorRecords = NULL;
	ISupportErrorInfo*      pISupportErrorInfo = NULL;
	ISQLErrorInfo*          pISQLErrorInfo = NULL;
	ISQLServerErrorInfo*    pISQLServerErrorInfo = NULL;

	// Number of error records.
	ULONG                   nRecs;
	ULONG                   nRec;

	// Basic error information from GetBasicErrorInfo.
	ERRORINFO               errorinfo;

	// IErrorInfo values.
	BSTR                    bstrDescription;
	BSTR                    bstrSource;

	// ISQLErrorInfo parameters.
	BSTR                    bstrSQLSTATE;
	LONG                    lNativeError;

	// ISQLServerErrorInfo parameter pointers.
	SSERRORINFO*            pSSErrorInfo = NULL;
	OLECHAR*                pSSErrorStrings = NULL;

	// Hard-code an American English locale for the example.
	DWORD                   MYLOCALEID = 0x0409;

	// Only ask for error information if the interface supports
	// it.
	if (FAILED(pObjectWithError->QueryInterface(IID_ISupportErrorInfo,
		(void**)&pISupportErrorInfo)))
	{
		wprintf_s(L"SupportErrorErrorInfo interface not supported");
		return;
	}
	if (FAILED(pISupportErrorInfo->
		InterfaceSupportsErrorInfo(IID_InterfaceWithError)))
	{
		wprintf_s(L"InterfaceWithError interface not supported");
		return;
	}

	// Do not test the return of GetErrorInfo. It can succeed and return
	// a NULL pointer in pIErrorInfoAll. Simply test the pointer.
	GetErrorInfo(0, &pIErrorInfoAll);

	if (pIErrorInfoAll != NULL)
	{
		// Test to see if it's a valid OLE DB IErrorInfo interface 
		// exposing a list of records.
		if (SUCCEEDED(pIErrorInfoAll->QueryInterface(IID_IErrorRecords,
			(void**)&pIErrorRecords)))
		{
			pIErrorRecords->GetRecordCount(&nRecs);

			// Within each record, retrieve information from each
			// of the defined interfaces.
			for (nRec = 0; nRec < nRecs; nRec++)
			{
				// From IErrorRecords, get the HRESULT and a reference
				// to the ISQLErrorInfo interface.
				pIErrorRecords->GetBasicErrorInfo(nRec, &errorinfo);
				pIErrorRecords->GetCustomErrorObject(nRec,
					IID_ISQLErrorInfo, (IUnknown**)&pISQLErrorInfo);

				// Display the HRESULT, then use the ISQLErrorInfo.
				wprintf_s(L"HRESULT:\t%#X\n", errorinfo.hrError);

				if (pISQLErrorInfo != NULL)
				{
					pISQLErrorInfo->GetSQLInfo(&bstrSQLSTATE,
						&lNativeError);

					// Display the SQLSTATE and native error values.
					wprintf_s(L"SQLSTATE:\t%s\nNative Error:\t%ld\n",
						bstrSQLSTATE, lNativeError);

					// SysFree BSTR references.
					SysFreeString(bstrSQLSTATE);

					// Get the ISQLServerErrorInfo interface from
					// ISQLErrorInfo before releasing the reference.
					pISQLErrorInfo->QueryInterface(
						IID_ISQLServerErrorInfo,
						(void**)&pISQLServerErrorInfo);

					pISQLErrorInfo->Release();
				}

				// Test to ensure the reference is valid, then
				// get error information from ISQLServerErrorInfo.
				if (pISQLServerErrorInfo != NULL)
				{
					pISQLServerErrorInfo->GetErrorInfo(&pSSErrorInfo,
						&pSSErrorStrings);

					// ISQLServerErrorInfo::GetErrorInfo succeeds
					// even when it has nothing to return. Test the
					// pointers before using.
					if (pSSErrorInfo)
					{
						// Display the state and severity from the
						// returned information. The error message comes
						// from IErrorInfo::GetDescription.
						wprintf_s(L"Error state:\t%d\nSeverity:\t%d\n",
							pSSErrorInfo->bState,
							pSSErrorInfo->bClass);

						// IMalloc::Free needed to release references
						// on returned values. For the example, assume
						// the g_pIMalloc pointer is valid.
						m_pIMalloc->Free(pSSErrorStrings);
						m_pIMalloc->Free(pSSErrorInfo);
					}

					pISQLServerErrorInfo->Release();
				}

				if (SUCCEEDED(pIErrorRecords->GetErrorInfo(nRec,
					MYLOCALEID, &pIErrorInfoRecord)))
				{
					// Get the source and description (error message)
					// from the record's IErrorInfo.
					pIErrorInfoRecord->GetSource(&bstrSource);
					pIErrorInfoRecord->GetDescription(&bstrDescription);

					if (bstrSource != NULL)
					{
						wprintf_s(L"Source:\t\t%s\n", bstrSource);
						SysFreeString(bstrSource);
					}
					if (bstrDescription != NULL)
					{
						wprintf_s(L"Error message:\t%s\n",
							bstrDescription);
						SysFreeString(bstrDescription);
					}

					pIErrorInfoRecord->Release();
				}
			}

			pIErrorRecords->Release();
		}
		else
		{
			// IErrorInfo is valid; get the source and
			// description to see what it is.
			pIErrorInfoAll->GetSource(&bstrSource);
			pIErrorInfoAll->GetDescription(&bstrDescription);

			if (bstrSource != NULL)
			{
				wprintf_s(L"Source:\t\t%s\n", bstrSource);
				SysFreeString(bstrSource);
			}
			if (bstrDescription != NULL)
			{
				wprintf_s(L"Error message:\t%s\n", bstrDescription);
				SysFreeString(bstrDescription);
			}
		}

		pIErrorInfoAll->Release();
	}
	else
	{
		wprintf_s(L"GetErrorInfo failed.");
	}

	pISupportErrorInfo->Release();

	return;
}

HRESULT CSimDB_ADO::quickInsert(void *pData)
{
	HRESULT hr;
	if (m_pIFastLoad)
		 hr = m_pIFastLoad->InsertRow(m_hAccessor, pData);
	if (FAILED(hr))
	{
		DumpErrorInfo(m_pIFastLoad, IID_ISQLServerErrorInfo);
	}
	return hr;
}

HRESULT SetFastLoadProperty(IDBInitialize * pIDBInitialize) {
	HRESULT hr = S_OK;
	IDBProperties * pIDBProps = NULL;
	DBPROP rgProps[1];
	DBPROPSET PropSet;

	VariantInit(&rgProps[0].vValue);

	rgProps[0].dwOptions = DBPROPOPTIONS_REQUIRED;
	rgProps[0].colid = DB_NULLID;
	rgProps[0].vValue.vt = VT_BOOL;
	rgProps[0].dwPropertyID = SSPROP_ENABLEFASTLOAD;

	rgProps[0].vValue.boolVal = VARIANT_TRUE;

	PropSet.rgProperties = rgProps;
	PropSet.cProperties = 1;
	PropSet.guidPropertySet = DBPROPSET_SQLSERVERDATASOURCE;

	if (SUCCEEDED(hr = pIDBInitialize->QueryInterface(IID_IDBProperties, (LPVOID *)&pIDBProps))) {
		hr = pIDBProps->SetProperties(1, &PropSet);
	}

	VariantClear(&rgProps[0].vValue);

	if (pIDBProps)
		pIDBProps->Release();

	return hr;
}

void SetupOption(DBPROPID PropID, const WCHAR *wszVal, DBPROP * pDBProp) {
	pDBProp->dwPropertyID = PropID;
	pDBProp->dwOptions = DBPROPOPTIONS_REQUIRED;
	pDBProp->colid = DB_NULLID;
	pDBProp->vValue.vt = VT_BSTR;
	pDBProp->vValue.bstrVal = SysAllocStringLen(wszVal, wcslen(wszVal));
}

HRESULT CSimDB_ADO::initQuickInsert(wstring ip, wstring user, wstring password, wstring db, wstring tableName)
{
	HRESULT hr = NOERROR;
	CoInitialize(NULL);
	const int cOPTION = 5;
	hr = CoGetMalloc(MEMCTX_TASK, &m_pIMalloc);
	if (FAILED(hr))
	{
		return hr;
	}
	DBPROPSET rgPropertySets[1];
	DBPROP rgDBProperties[cOPTION];
	rgPropertySets[0].rgProperties = rgDBProperties;
	rgPropertySets[0].cProperties = cOPTION;
	rgPropertySets[0].guidPropertySet = DBPROPSET_DBINIT;

	SetupOption(DBPROP_INIT_CATALOG, db.c_str(), &rgDBProperties[0]);
	SetupOption(DBPROP_INIT_DATASOURCE, ip.c_str(), &rgDBProperties[1]);
	SetupOption(DBPROP_AUTH_PASSWORD, password.c_str(), &rgDBProperties[3]);
	SetupOption(DBPROP_AUTH_USERID, user.c_str(), &rgDBProperties[4]);
	m_pIDBInitialize = NULL;
	if (!SUCCEEDED(hr = CoCreateInstance(SQLNCLI_CLSID, NULL, CLSCTX_INPROC_SERVER, IID_IDBInitialize, (void **)&m_pIDBInitialize)))
	{
		return hr;
	}
	IDBProperties* pIDBProperties = NULL;
	if (!SUCCEEDED(hr = m_pIDBInitialize->QueryInterface(IID_IDBProperties,(void **)&pIDBProperties)))
	{
		return hr;
	}
	if (!SUCCEEDED(hr = pIDBProperties->SetProperties(1, rgPropertySets)))
	{
		return hr;
	}
	hr = m_pIDBInitialize->Initialize();
	if (FAILED(hr))
	{
		return hr;
	}
	pIDBProperties->Release();

	DBID TableID;
	TableID.eKind = DBKIND_NAME;
	wstring newTableName = L"["+tableName+L"]";
	LPOLESTR x = TableID.uName.pwszName = SysAllocStringLen(newTableName.c_str(), newTableName.size());

	hr = SetFastLoadProperty(m_pIDBInitialize);
	if (FAILED(hr))
	{
		return hr;
	}

	m_pIDBCreateSession = NULL;
	if (!SUCCEEDED(hr = m_pIDBInitialize->QueryInterface(IID_IDBCreateSession, (void**)&m_pIDBCreateSession)))
	{
		return hr;
	}

	IOpenRowset* pIOpenRowsetFL = NULL;
	m_pIFastLoad = NULL;
	hr = m_pIDBCreateSession->CreateSession(NULL, IID_IOpenRowset, (IUnknown **)&pIOpenRowsetFL);
	if (FAILED(hr))
	{
		return hr;
	}

	if (FAILED(hr = pIOpenRowsetFL->OpenRowset(NULL, &TableID, NULL, IID_IRowsetFastLoad, 0, NULL, (LPUNKNOWN *)&m_pIFastLoad)))
	{
		return hr;
	}

	pIOpenRowsetFL->Release();
	IAccessor* pIAccessor = NULL;
	m_hAccessor = 0;
	hr = m_pIFastLoad->QueryInterface(IID_IAccessor, (void **)&pIAccessor);
	if (FAILED(hr))
	{
		return hr;
	}
	DBORDINAL  pcColumns;
	DBBINDING *rgBindings = NULL;
	hr = bindData(&pcColumns, &rgBindings);
	if (FAILED(hr))
	{
		return hr;
	}
	hr = pIAccessor->CreateAccessor(DBACCESSOR_ROWDATA, pcColumns, rgBindings, 0, &m_hAccessor, NULL);
	pIAccessor->Release();
	return hr;
}

HRESULT CSimDB_ADO::bindData(DBORDINAL  *pcColumns, DBBINDING **ppBinding)
{
	HRESULT hr;
	IAccessor* pIAccessor = NULL;
	hr = m_pIFastLoad->QueryInterface(IID_IAccessor, (void **)&pIAccessor);
	if (FAILED(hr))
	{
		return hr;
	}
	IColumnsInfo *pIColumnsInfo = NULL;
	hr = pIAccessor->QueryInterface(IID_IColumnsInfo, (void **)&pIColumnsInfo);
	if (FAILED(hr))
		return hr;

	DBCOLUMNINFO *prgInfo;
	OLECHAR      *pStringsBuffer;
	pIColumnsInfo->GetColumnInfo(pcColumns, &prgInfo, &pStringsBuffer);
	DBBINDING *rgBindings = NULL;
	DBORDINAL cols = *pcColumns;
	rgBindings = (DBBINDING*)malloc(cols * sizeof(DBBINDING));
	memset(rgBindings, 0, cols * sizeof(DBBINDING));
	ULONG dwOffset = 0;
	for (ULONG iCol = 0; iCol < cols; iCol++)
	{
		if (prgInfo[iCol].wType == DBTYPE_WSTR)
		{
			rgBindings[iCol].wType = DBTYPE_STR;
		}
		else
		{
			rgBindings[iCol].wType = prgInfo[iCol].wType;
		}
		//如果是字符串类型则不需要对齐
		if (DBTYPE_STR != rgBindings[iCol].wType)
		{
			if (COLUMN_ALIGNVAL_8 == prgInfo[iCol].ulColumnSize)
			{
				dwOffset = ROUND_UP(dwOffset, COLUMN_ALIGNVAL_8);//结构体对齐,根据相应的关键设置对齐方式.
			}
			else if (COLUMN_ALIGNVAL_4 == prgInfo[iCol].ulColumnSize)
			{
				dwOffset = ROUND_UP(dwOffset, COLUMN_ALIGNVAL_4);//结构体对齐,根据相应的关键设置对齐方式.
			}
		}

		rgBindings[iCol].dwPart = DBPART_VALUE;
		rgBindings[iCol].iOrdinal = prgInfo[iCol].iOrdinal;
		rgBindings[iCol].pTypeInfo = NULL;
		rgBindings[iCol].pObject = NULL;
		rgBindings[iCol].pBindExt = NULL;
		rgBindings[iCol].dwFlags = 0;
		rgBindings[iCol].obValue = dwOffset;
		rgBindings[iCol].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
		rgBindings[iCol].eParamIO = DBPARAMIO_NOTPARAM;
		rgBindings[iCol].bPrecision = prgInfo[iCol].bPrecision;
		rgBindings[iCol].bScale = prgInfo[iCol].bScale;		
		rgBindings[iCol].cbMaxLen = prgInfo[iCol].ulColumnSize;
		dwOffset = rgBindings[iCol].cbMaxLen + rgBindings[iCol].obValue;
	}
	pIAccessor->Release();
	pIColumnsInfo->Release();
	CoTaskMemFree(prgInfo);
	CoTaskMemFree(pStringsBuffer);
	*ppBinding = rgBindings;
	return hr;
}

HRESULT CSimDB_ADO::quickInsertCommit()
{
	HRESULT hr = m_pIFastLoad->Commit(TRUE);
	if (FAILED(hr))
	{
		DumpErrorInfo(m_pIFastLoad, IID_ISQLServerErrorInfo);
	}
	return hr;
}

void CSimDB_ADO::finitQuickInsert()
{
	if (m_pIFastLoad)
	{
		m_pIFastLoad->Release();
		m_pIFastLoad = NULL;
	}

	if (m_pIDBInitialize)
	{
		m_pIDBInitialize->Uninitialize();
		m_pIDBInitialize = NULL;
	}
	if (m_pIDBCreateSession)
	{
		m_pIDBCreateSession->Release();
		m_pIDBCreateSession = NULL;
	}
	m_hAccessor = 0;
	if(m_pIMalloc)
	{
		m_pIMalloc->Release();
	}
	CoUninitialize();
}

