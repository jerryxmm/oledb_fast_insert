//////////////////////////////////////////////////////////////////////////
//
// 1. #import "C:/Program Files (x86)/Common Files/System/ado/msado15.dll"
//         no_namespace rename("EOF","adoEOF")
//	编译之后拷贝生成的文件并使用 #include "msado15.tlh"
//	也可以直接使用上面的 #import
// 
// 2. CoInitialize 需要先初始化COM
//
// 3. important!!! SQLSERVER参数变量必须根据变量占位符顺序设置,字段名暂时不起作用
//     db.Cmd("insert into [user] values(?,?,?)");
//     db.Set("", p1);
//     db.Set("", p2);
//////////////////////////////////////////////////////////////////////////
#pragma once

#ifndef __SIMDB_ODBC_H__
#define __SIMDB_ODBC_H__

#define WIN32_LEAN_AND_MEAN
#include <string>
#include <comdef.h>

//#include "AxCom.h"

using namespace std;
#include <oledb.h>
#include <oledberr.h>
#include <msdasc.h>
#include <msdaguid.h>
#include <msdasql.h>
#include "C:\Program Files (x86)\Microsoft SQL Server\100\SDK\Include\sqlncli.h"

//#import "C:/Program Files (x86)/Common Files/System/ado/msado15.dll"  no_namespace rename("EOF","adoEOF")

#define DB_TYPE_ORACLE		1
#define	DB_TYPE_SQLSERVER	2
#define	DB_TYPE_SYBASE		3
#define	DB_TYPE_DB2			4
#define	DB_TYPE_MYSQL		5
#define	DB_TYPE_DBLIBRARY	6

class CSimDB_ADO 
{
public:    
	CSimDB_ADO();
    ~CSimDB_ADO();

public:

	HRESULT initQuickInsert(wstring ip, wstring user, wstring password, wstring db, wstring tableName);
	HRESULT quickInsert(void *pData);
	HRESULT quickInsertCommit();
	void finitQuickInsert();
private:
	void DumpErrorInfo(IUnknown* pObjectWithError, REFIID IID_InterfaceWithError);
	HRESULT bindData(DBORDINAL  *pcColumns, DBBINDING **rgBindings);
private:
// 	string m_svr;
// 	string m_susr;
// 	string m_spwd;
// 	string m_sdb;
// 	string m_src; // 调用源

	IRowsetFastLoad* m_pIFastLoad;
	HACCESSOR m_hAccessor;
	IDBInitialize *m_pIDBInitialize;
	IDBCreateSession *m_pIDBCreateSession;
	IMalloc* m_pIMalloc;
};

#endif // __SIMDB_ODBC_H__
