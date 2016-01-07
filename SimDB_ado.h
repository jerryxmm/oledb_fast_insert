//////////////////////////////////////////////////////////////////////////
//
// 1. #import "C:/Program Files (x86)/Common Files/System/ado/msado15.dll"
//         no_namespace rename("EOF","adoEOF")
//	����֮�󿽱����ɵ��ļ���ʹ�� #include "msado15.tlh"
//	Ҳ����ֱ��ʹ������� #import
// 
// 2. CoInitialize ��Ҫ�ȳ�ʼ��COM
//
// 3. important!!! SQLSERVER��������������ݱ���ռλ��˳������,�ֶ�����ʱ��������
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
#include "C:\Program Files (x86)\Microsoft SQL Server\110\SDK\Include\sqlncli.h"

#import "C:/Program Files (x86)/Common Files/System/ado/msado15.dll"  no_namespace rename("EOF","adoEOF")

#define DB_TYPE_ORACLE		1
#define	DB_TYPE_SQLSERVER	2
#define	DB_TYPE_SYBASE		3
#define	DB_TYPE_DB2			4
#define	DB_TYPE_MYSQL		5
#define	DB_TYPE_DBLIBRARY	6
// @type UWORD    | 2 byte unsigned integer.
typedef unsigned short UWORD;

// @type SDWORD   | 4 byte signed integer.
typedef signed long SDWORD;
class CSimDB_ADO 
{
public:    
	CSimDB_ADO();
	virtual ~CSimDB_ADO();

public:
    bool Open( const string &svr, const string &usr, const string &pwd, const string &db);
    bool Reopen( const string &svr, const string &usr, const string &pwd, const string &db);
    void Close();
    
	void Cmd(const char *cmd, ...);
	//NOTE: important!!! SQLSERVER����������ݱ���ռλ��˳������,�ֶ�����ʱ��������
	void Set(const string &sField, const string &sValue);
	void Set(const string &sField, double dValue);
	void Set(const string &sField, long nValue);
	void Set(const string &sField, int nValue);
	void Set(const string &sField, BYTE *pBuf, int nLen);
	void SetNull(const string &sField);
	
	void SetDBType(int nDBType);
	void SetUserMode(const string &sMode);
	void SetValue(int nType, const string &sVal);
	
	void SetReq(void *pInput);
	bool SetSQLSize(int nSize);
	bool SetReqBuf(byte *pBuf, int nSize);

	string GetSvr();
	string GetUser();
	string GetPwd();
	string GetDB();
	string GetUserMode();
	string Get(int id);
	string Get(const string &name);
	string GetColName(int nFiledIndex);
	string GetColType(int index);
	string GetHeadScript();
	string GetLastErrMsg();
	string GetValue(int nType);
	double GetDouble(int id);
	double GetDouble(const string &name);
	long GetLong(int id);
	long GetLong(const string &name);
	int GetDataBaseType();
	char* GetSql();

    bool GetBuffer( const string &sField, BYTE *pBuf, long &nSize );
    bool GetBuffer( int	nFieldIndex, BYTE *pBuf, long &nSize );
    long GetDataSize( const string &sField );
    long GetDataSize( int nFieldIndex );
    long GetActualSize( const string &sField );
    long GetActualSize( int nFieldIndex );
    long GetColIndex( const string &sField );
	long GetCount();
    int	GetNumCols();
	int GetCode();

	bool SelectInto(CStdRecord *pRecord, bool bCreateHead = true);

	HRESULT initFastInsert(wstring ip, wstring user, wstring password, wstring tableName);
	HRESULT fastInsertRow(void *pData);
	HRESULT fastInsertCommit();
	void finitFastInsert();
public:
	long Exec();
	bool More();	
	bool IsTran();
	void BeginTrans();
	void RollBack();
	void Commit();
	bool IsDead();
	bool IsError();
	void ClearSql();

public:		
    int  m_nDBType; // δʹ��
    void *m_pDB; // δʹ��
    void *m_pRecv;
    string m_sErr;
    int m_nCode;

public:
	//SQL Server��ʱ��֧��row_��ͷ�ĺ���
	int  row_getsize();
	bool row_gethead(string &sHead);
	bool row_more(char *pData, int &nSize);

	void SetAutoCommit(bool autoCommit); // not support
	bool MoreForUpdate(); // not support
	bool SetBuffer(int	nFieldIndex, BYTE *pBuf, long nSize); // not support
	bool SetBuffer(const string &sField, BYTE *pBuf, long nSize);// not support
	void registerOutParameter(const string &sField, char cType); // not support

private:
	char* GetSQLPos();
	bool LoadDB();
	bool ResetDB();
	bool ExecNoMore();
	bool CheckDBConn();
	void WriteSQLLog();
	void Proc_Error(_com_error& e, long nLine);

private:
	string m_svr;
	string m_susr;
	string m_spwd;
	string m_sdb;
	string m_src; // ����Դ
	string m_susermode; // ��֧��

	byte* m_pReqBuf; // ��֧��
	int m_nReqBufSize;
	void* m_pReq;

	bool m_bError;
	bool m_bReopen;
	bool m_bAutoCommit;// ��֧��
	char* m_pSql;
	int m_nSqlPos;
	int m_nSqlSize;
	int m_nStatus; // 0 :SQL ����״̬ 1 :��SQL���ȴ�ִ�� 2:�н�����ݴ�ȡ��
	long m_nCount;
	long m_nCmdOptions;

	class AdoObj;
	AdoObj* m_ado;
	IRowsetFastLoad* m_pIFastLoad;
	HACCESSOR m_hAccessor;
	IDataInitialize *m_pIDataInitialize;
	IDBInitialize *m_pIDBInitialize;
	IDBCreateSession *m_pIDBCreateSession;
};

#endif // __SIMDB_ODBC_H__
