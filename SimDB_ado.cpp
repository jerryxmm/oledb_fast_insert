#include "StdAfx.h"
#include "SimDB_ado.h"

//#include "msado15.tlh"
#include <time.h>

#include <axcom/CommFunc.h>
#include <axcom/AxEvent.h>
#include <axstd/stdstring.h>
#include <iostream>
#define DB_TIMEOUT 10000
#define PROCESS_ERROR(e) Proc_Error(e, __LINE__)
#define COLUMN_ALIGNVAL 8
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

class AdoConnEvent : public ConnectionEventsVt
{
public:
	ULONG m_cRef;
	void* m_owner;
	ado_callback m_cb;

public:
	AdoConnEvent() 
	{
		m_cRef = 0;
		m_owner = NULL;
		m_cb = NULL;
	};
	~AdoConnEvent() {};

	STDMETHODIMP QueryInterface(REFIID riid, void ** ppv)
	{
		*ppv = NULL;
		if (riid == __uuidof(IUnknown) || riid == __uuidof(ConnectionEventsVt))
			*ppv = this;

		if (*ppv == NULL)
			return ResultFromScode(E_NOINTERFACE);

		AddRef();
		return NOERROR;
	}
	STDMETHODIMP_(ULONG) AddRef()
	{
		return ++m_cRef;
	}
	STDMETHODIMP_(ULONG) Release()
	{
		if (0 != --m_cRef)
			return m_cRef;
		delete this;
		return 0;
	}

	STDMETHODIMP raw_InfoMessage(struct Error *pError,
		EventStatusEnum *adStatus,
		struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 1, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

	STDMETHODIMP raw_BeginTransComplete(LONG TransactionLevel,
		struct Error *pError,
		EventStatusEnum *adStatus,
		struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 2, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

	STDMETHODIMP raw_CommitTransComplete(struct Error *pError,
		EventStatusEnum *adStatus,
		struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 3, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}
	
	STDMETHODIMP raw_RollbackTransComplete(struct Error *pError,
		EventStatusEnum *adStatus,
		struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 4, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

	STDMETHODIMP raw_WillExecute(BSTR *Source,
		CursorTypeEnum *CursorType,
		LockTypeEnum *LockType,
		long *Options,
		EventStatusEnum *adStatus,
		struct _Command *pCommand,
		struct _Recordset *pRecordset,
		struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 5, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

	STDMETHODIMP raw_ExecuteComplete(LONG RecordsAffected,
		struct Error *pError,
		EventStatusEnum *adStatus,
		struct _Command *pCommand,
		struct _Recordset *pRecordset,
		struct _Connection *pConnection)
	{
		if (m_cb)
			m_cb(m_owner, ADOEV_EXECCOMPLETE, pCommand);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

	STDMETHODIMP raw_WillConnect(BSTR *ConnectionString,
		BSTR *UserID,
		BSTR *Password,
		long *Options,
		EventStatusEnum *adStatus,
		struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 7, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

	STDMETHODIMP raw_ConnectComplete(struct Error *pError,
		EventStatusEnum *adStatus,
		struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 8, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

	STDMETHODIMP raw_Disconnect(EventStatusEnum *adStatus, struct _Connection *pConnection)
	{
		//if (m_cb)
		//	m_cb(m_owner, 9, 0);
		*adStatus = adStatusUnwantedEvent;
		return S_OK;
	}

};


class CSimDB_ADO::AdoObj
{
public:
	AdoObj()
	{
		conn = NULL;
		rec = NULL;
		cmd = NULL;
		m_evcmd.Reset();
		m_bSetupEvent = false;
		m_evhandler = NULL;
		dwConnEvt = 0;
	}
	~AdoObj()
	{
		ResetCommand();
		ResetRecordset();
		ResetConnection();
	}

public:
	_ConnectionPtr conn;
	_RecordsetPtr rec;
	_CommandPtr cmd;
	CAxEvent m_evcmd;
	AdoConnEvent* m_evhandler;
	bool m_bSetupEvent;
	DWORD dwConnEvt;
public:
	static void on_ado_callback(void* owner, int ev, void* data)
	{
		if (owner)
		{
			CSimDB_ADO::AdoObj* _this = (CSimDB_ADO::AdoObj *) owner;
			_this->on_callback(ev, data);
		}
	}
	void on_callback(int ev, void* data)
	{
		if (ev == ADOEV_EXECCOMPLETE)
		{
			//if (data == cmd.GetInterfacePtr())
			{
				m_evcmd.Set();
			}
		}
	}
	void SetupEvents()
	{
		if (m_bSetupEvent || !conn)
		{
			return;
		}

		IConnectionPointContainer *pCPC = NULL;
		IConnectionPoint *pCP = NULL;
		IUnknown *pUnk = NULL;
		
		HRESULT hr = conn->QueryInterface(__uuidof(IConnectionPointContainer), (void **)&pCPC);
		if (FAILED(hr) || pCPC == NULL)
			return;

		hr = pCPC->FindConnectionPoint(__uuidof(ConnectionEvents), &pCP);
		pCPC->Release();

		if (FAILED(hr) || pCP == NULL)
			return;

		m_evhandler = new AdoConnEvent();
		hr = m_evhandler->QueryInterface(__uuidof(IUnknown), (void **)&pUnk);

		if (FAILED(hr))
		{
			delete m_evhandler;
			m_evhandler = NULL;
			return;
		}

		hr = pCP->Advise(pUnk, &dwConnEvt);
		pCP->Release();

		if (FAILED(hr))
		{
			pUnk->Release();
			delete m_evhandler;
			m_evhandler = NULL;
			return;
		}
		m_evhandler->m_cb = on_ado_callback;
		m_evhandler->m_owner = this;
		m_bSetupEvent = true;
	}
	void RemoveEvents()
	{
		if (!m_bSetupEvent || m_evhandler == NULL || !conn)
			return;

		m_bSetupEvent = false;

		IConnectionPointContainer *pCPC = NULL;
		IConnectionPoint *pCP = NULL;

		HRESULT hr = conn->QueryInterface(__uuidof(IConnectionPointContainer), (void **)&pCPC);

		if (FAILED(hr) || pCPC == NULL)
			return;

		hr = pCPC->FindConnectionPoint(__uuidof(ConnectionEvents), &pCP);
		pCPC->Release();

		if (FAILED(hr) || pCP == NULL)
			return;

		hr = pCP->Unadvise(dwConnEvt);
		pCP->Release();
	}
	void WaitCommandExec()
	{
		if (m_bSetupEvent && m_evhandler)
			m_evcmd.Wait();
	}
	void CheckCommand()
	{
		if (!cmd)
		{
			cmd = _CommandPtr(__uuidof(Command));
			cmd->ActiveConnection = conn;
		}
	}
	void CheckRecordset()
	{
		if (!rec)
		{
			rec = _RecordsetPtr(__uuidof(Recordset));			
		}
	}
	void ResetCommand()
	{
		try
		{
			if (cmd)
			{
				if (cmd->State == adStateOpen)
					cmd->Cancel();
				cmd = NULL;
			}
		}
		catch (_com_error& e)
		{
			(e);
			cmd = NULL;
		}
	}
	void ResetRecordset()
	{
		try
		{
			if (rec)
			{
				if (rec->State == adStateOpen)
					rec->Close();
				rec = NULL;
			}
		}
		catch (_com_error& e)
		{
			(e);
			rec = NULL;
		}
	}
	void ResetConnection()
	{
		try	
		{
			if (conn)
			{
				RemoveEvents();

				if (conn->State == adStateOpen)
					conn->Close();
				conn = NULL;
			}
		}
		catch (_com_error& e)
		{
			(e);
			conn = NULL;
		}
	}
	bool IsRecordsetNoData()
	{
		return (!rec || !rec->State || rec->adoEOF);
	}
	string GetInputParams()
	{
		string rs;
		if (cmd)
		{
			long c = cmd->Parameters->Count;
			for (long i = 0; i < c; i ++)
			{
				_ParameterPtr p = cmd->Parameters->GetItem(_variant_t(i));
				if (p->Direction == adParamInput
					|| p->Direction == adParamInputOutput)
				{
					rs += p->Name;
					rs += "|";
				}
			}
		}
		return rs;
	}
};

CSimDB_ADO::CSimDB_ADO()
{
    m_pDB = NULL;
    m_pRecv = NULL;
    m_nDBType = DB_TYPE_SQLSERVER;
	m_nCode = 0;
	m_nStatus = 0;
	m_pReqBuf = NULL;
	m_nReqBufSize = 0;
	m_pReq = NULL;

	m_ado = new AdoObj();
	m_nSqlPos = 0;
	m_pSql = NULL;
	m_bReopen = false;
	m_nSqlSize = 16 * 1024 + 1;
	m_pSql = new char[m_nSqlSize]();
	m_nCount = 0;
	m_nCmdOptions = adCmdText;
	m_bAutoCommit = false;
	m_bError = false;
	m_pIFastLoad = NULL;
	m_hAccessor = 0;
	m_pIDataInitialize = NULL;
	m_pIDBInitialize = NULL;
	m_pIDBCreateSession = NULL;
}

CSimDB_ADO::~CSimDB_ADO()
{
    Close();
	delete m_ado;
	delete[] m_pSql;
}

bool CSimDB_ADO::Open( const string &svr, const string &usr, const string &pwd, const string &db)
{
    if (LoadDB() == false)
        return false;
 
	bool rs = false;
	bool reopen = m_bReopen;
	m_bReopen = false;
	try
	{
		char buf[1025] = { 0 };
		sprintf_s(buf, 1024,"Provider=SQLOLEDB.1;Data Source=%s;Initial Catalog=%s;", 
			svr.c_str(), db.c_str());

		HRESULT hr = m_ado->conn->Open(buf, usr.c_str(), pwd.c_str(), adConnectUnspecified);
		if (FAILED(hr))
		{
			m_nCode = GetLastError();
			m_sErr = "ADO 打开数据库失败!";
		} else {
			rs = true;
			m_svr = svr;
			m_susr = usr;
			m_spwd = pwd;
			m_sdb = db;
			m_ado->CheckCommand();
			m_ado->CheckRecordset();
			//m_ado->SetupEvents();
		}
	}
	catch (_com_error& e)
	{
		PROCESS_ERROR(e);
		return false;
	}
	catch (...)
	{
		m_nCode = GetLastError();
		m_sErr = "ADO 打开数据库异常!!";
	}

	if (rs == false)
	{
		m_ado->conn = NULL;
	}
	else if (reopen)
	{
		string sNote = "";
		char tmp[100];
		sprintf_s(tmp, 99, "[%li %s] ", GetCurDate(), GetCurTime().c_str());
		sNote = tmp;
		sNote += "用户[" + m_susr + "]与数据库[" + m_svr + "]连接恢复.";

		sprintf_s(tmp, 99, "(Ptr=%li)(Thr=%li)\n", (long)this, ::GetCurrentThreadId());
		sNote += tmp;

		UniWriteLog(sNote);
	}
	return rs;
}

void CSimDB_ADO::Close()
{
	m_ado->ResetCommand();
	m_ado->ResetRecordset();
	m_ado->ResetConnection();
}

void CSimDB_ADO::Cmd( const char *cmd,... )
{    
	if (LoadDB() == false)
		return;

	if (ResetDB() == false)
		return;

	char* pc = GetSQLPos();
	va_list s1;
	va_start(s1, cmd);
	vsprintf(pc, cmd, s1);
	va_end(s1);
}

void CSimDB_ADO::Set( const string &sField, const string &sValue )
{
	if (!CheckDBConn())
		return;

	string value = sValue;
	if (value.size() >= 4096)
	{
		CStdString sNote;
		sNote.Format("[%li %s] 设置数据越界超过4096.[%s]=[%s]\n",
			GetCurDate(), GetCurTime().c_str(),
			sField.c_str(), sValue.c_str());
		UniWriteLog(sNote.c_str());
		
		value = value.substr(0, 4090);
	}

	try
	{
		m_ado->CheckCommand();

		_ParameterPtr p = m_ado->cmd->CreateParameter(sField.c_str(), adBSTR, adParamInput, (long)sValue.length(), sValue.c_str());
		if (p == NULL || FAILED(m_ado->cmd->Parameters->Append(p)))
		{
			m_nCode = -1;
			m_sErr = "创建字段失败: " + sField;
		}
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);
	}
}

void CSimDB_ADO::Set( const string &sField, double dValue )
{
	if (!CheckDBConn())
		return;

	try
	{
		m_ado->CheckCommand();

		_ParameterPtr p = m_ado->cmd->CreateParameter(sField.c_str(), adDouble, adParamInput, sizeof(double), dValue);
		if (p == NULL || FAILED(m_ado->cmd->Parameters->Append(p)))
		{
			m_nCode = -1;
			m_sErr = "创建字段失败: " + sField;
		}
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);
	}
}

void CSimDB_ADO::Set( const string &sField, long nValue )
{
	if (!CheckDBConn())
		return;

	try
	{
		m_ado->CheckCommand();

		_ParameterPtr p = m_ado->cmd->CreateParameter(sField.c_str(), adBigInt, adParamInput, sizeof(long), nValue);
		if (p == NULL || FAILED(m_ado->cmd->Parameters->Append(p)))
		{
			m_nCode = -1;
			m_sErr = "创建字段失败: " + sField;
		}
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);
	}
}

void CSimDB_ADO::Set( const string &sField, int nValue )
{
	if (!CheckDBConn())
		return;

	try
	{
		m_ado->CheckCommand();

		_ParameterPtr p = m_ado->cmd->CreateParameter(sField.c_str(), adInteger, adParamInput, sizeof(int), nValue);
		if (p == NULL || FAILED(m_ado->cmd->Parameters->Append(p)))
		{
			m_nCode = -1;
			m_sErr = "创建字段失败: " + sField;
		}
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);
	}
}

void CSimDB_ADO::Set( const string &sField, BYTE *pBuf, int nLen )
{
	if (pBuf != NULL)
	{
		string sValue;
		sValue.append((char *)pBuf, nLen);
		Set(sField, sValue);
	}
}

void CSimDB_ADO::SetNull( const string &sField )
{
	if (!CheckDBConn())
		return;

	try
	{
		m_ado->CheckCommand();

		_ParameterPtr p = m_ado->cmd->CreateParameter(sField.c_str(), adBSTR, adParamInput, 0);
		if (p == NULL || FAILED(m_ado->cmd->Parameters->Append(p)))
		{
			m_nCode = -1;
			m_sErr = "创建字段失败: " + sField;
		}
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);
	}
}

void CSimDB_ADO::registerOutParameter( const string &sField, char cType )
{
	//TODO: 暂时不支持
	m_nCode = -1;
	m_sErr = "暂时不支持.";
	return;

	if (!CheckDBConn())
		return;

	try
	{

		m_ado->CheckCommand();

		_ParameterPtr p;
		switch (cType)
		{
		case 'I':
			p = m_ado->cmd->CreateParameter(sField.c_str(), adInteger, adParamOutput, 0);			
			break;
		case 'L':
			p = m_ado->cmd->CreateParameter(sField.c_str(), adBigInt, adParamOutput, 0);
			break;
		case 'R':
			p = m_ado->cmd->CreateParameter(sField.c_str(), adDecimal, adParamOutput, 0);
			break;
		case 'C':
			p = m_ado->cmd->CreateParameter(sField.c_str(), adBSTR, adParamOutput, 0);
			break;
		case 'F':
			p = m_ado->cmd->CreateParameter(sField.c_str(), adDouble, adParamOutput, 0);
			break;
		default:
			p = m_ado->cmd->CreateParameter(sField.c_str(), adInteger, adParamOutput, 0);
		}
		if (p == NULL || FAILED(m_ado->cmd->Parameters->Append(p)))
		{
			m_nCode = -1;
			m_sErr = "创建OUT字段失败: " + sField;
		}
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);
	}
}

bool CSimDB_ADO::More()
{
	m_sErr = "";
	if (m_nStatus == 0)
		return false;

	bool bFirst = (m_nStatus == 1);
	if (ExecNoMore() == false)
	{
		return false;
	}

	try
	{
		if (m_ado->IsRecordsetNoData())
		{
			return false;
		}

		//TODO: 直接more的用法导致忽略第一条记录,检查这个标记是否合理
		if (!bFirst)
			m_ado->rec->MoveNext();

		if (!m_ado->IsRecordsetNoData())
		{
			m_nCount++;
			return true;
		}
	}
	catch (_com_error &e)
	{
		//strcpy( m_cSQL + strlen( m_cSQL ), "\r\n" );
		//UniWriteLog( m_cSQL );
		PROCESS_ERROR(e);
		m_nStatus = 0;
		return false;
	}
	m_nStatus = 0;
	return false;
}

bool CSimDB_ADO::ExecNoMore()
{
	if (!CheckDBConn())
		return false;

	if (m_nStatus == 1)
	{
		try
		{
			m_ado->CheckCommand();
			m_ado->CheckRecordset();

			clock_t start = clock();
			
			_variant_t vr;
			m_ado->cmd->PutCommandText(_bstr_t(m_pSql));
			m_ado->rec = m_ado->cmd->Execute(&vr, NULL, m_nCmdOptions);

			if (clock() - start > DB_TIMEOUT)
			{
				CStdString sNote;
				sNote.Format("ThrId=%li语句查询超时%.3f秒", ::GetCurrentThreadId(), double(clock() - start) / 1000);
				m_sErr = sNote.c_str();
				WriteSQLLog();
			}
			
			std::memset(m_pSql, 0, sizeof(m_pSql));
		}
		catch (_com_error &e)
		{
			PROCESS_ERROR(e);
			std::memset(m_pSql, 0, sizeof(m_pSql));
			m_nStatus = 0;
			m_ado->ResetRecordset();
			return false;
		}
		m_nStatus = 2; // have results need to process
		return true;
	}
	return true;
}

long CSimDB_ADO::Exec()
{
	m_nCode = 0;
	m_nCount = 0;
	m_sErr.clear();
	
	if (!CheckDBConn())
		return 0;

	try
	{
		try
		{
			m_ado->CheckCommand();

			clock_t start = clock();			
			
			_variant_t vr;
			m_ado->cmd->PutCommandText(_bstr_t(m_pSql));
			m_ado->rec = m_ado->cmd->Execute(&vr, NULL, m_nCmdOptions);
			//m_ado->WaitCommandExec();

			if (clock() - start > DB_TIMEOUT)
			{
				CStdString sNote;
				sNote.Format("ThrId=%li语句执行超时%.3f秒", ::GetCurrentThreadId(), double(clock() - start) / 1000);
				m_sErr = sNote.c_str();
				WriteSQLLog();
			}

			std::memset(m_pSql, 0, sizeof(m_pSql));

			m_nCount = vr.intVal;
			if (!m_ado->IsRecordsetNoData())
			{
				m_nCount = m_ado->rec->RecordCount;
			}
		}
		catch (_com_error &e)
		{
			PROCESS_ERROR(e);
			std::memset(m_pSql, 0, sizeof(m_pSql));
			return 0;
		}
	}
	catch (...)
	{
		CStdString sNote;
		sNote.Format("[%li %s] 语句执行异常SQL!\n", GetCurDate(), GetCurTime().c_str());
		UniWriteLog(sNote.c_str());
	}

	return m_nCount;
}

bool CSimDB_ADO::SelectInto(CStdRecord *pRecord, bool bCreateHead)
{
	if (!CheckDBConn() || strlen(m_pSql) <= 0)
		return false;

	m_nCount = 0;
	DATE tm;
	DATETIME mDT;
	CStdString sValue, sHead;

	try
	{
		m_ado->CheckCommand();
		m_ado->CheckRecordset();

		clock_t start = clock();

		_variant_t vr;
		m_ado->cmd->PutCommandText(_bstr_t(m_pSql));
		m_ado->rec = m_ado->cmd->Execute(&vr, NULL, m_nCmdOptions);

		if (clock() - start > DB_TIMEOUT)
		{
			CStdString sNote;
			sNote.Format("ThrId=%li语句SelectInto查询超时%.3f秒", ::GetCurrentThreadId(), double(clock() - start) / 1000);
			m_sErr = sNote.c_str();
			WriteSQLLog();
		}
		
		std::memset(m_pSql, 0, sizeof(m_pSql));

		FieldPtr p;
		long nFieldCount = m_ado->rec->GetFields()->GetCount();

		if (bCreateHead)
		{
			string sHead;
			CStdString sValue;

			for (long i = 0; i < nFieldCount; i++)
			{
				p = m_ado->rec->GetFields()->GetItem(_variant_t(i));
				sValue = "";

				switch (p->GetType())
				{
				case adTinyInt:
				case adSmallInt:
				case adInteger:
				case adBigInt:
				case adUnsignedTinyInt:
				case adUnsignedSmallInt:
				case adUnsignedInt:
				case adUnsignedBigInt:
				case adNumeric:
					sValue.Format("%s[I]", (char *)p->Name);
					break;
				case adBSTR:
				case adChar:
				case adVarChar:
				case adLongVarChar:
				case adWChar:
				case adVarWChar:
				case adLongVarWChar:
				case adBoolean:
					sValue.Format("%s[C%li]", (char *)p->Name, p->DefinedSize);
					break;
				case adDate:	// DATE
				case adDBDate:
				case adDBTime:
				case adDBTimeStamp:
					sValue.Format("%s[D]", (char *)p->Name);
					break;
				case adDecimal:	// FLOAT
					if (p->NumericScale == 0)
					{
						if (p->Precision == 0)
							sValue.Format("%s[R16.3]", (char *)p->Name);
						else
							sValue.Format("%s[I]", (char *)p->Name);
					}
					else
						sValue.Format("%s[R12.%li]", (char *)p->Name, p->NumericScale);
					break;
				case adSingle:	// FLOAT  
				case adDouble:	// FLOAT  
					sValue.Format("%s[R12.%li]", (char *)p->Name, p->NumericScale);
					break;
				}
				if (sHead.size() > 0) sHead += ",";
				sHead += sValue.c_str();
			}
			pRecord->Empty();
			pRecord->Create(sHead.c_str());
		}

		while (!m_ado->IsRecordsetNoData())
		{
			pRecord->Append();
			m_nCount++;

			for (long i = 0; i < nFieldCount; i++)
			{
				p = m_ado->rec->GetFields()->GetItem(_variant_t(i));
				sValue = "";

				switch (p->GetType())
				{
				case adTinyInt:
				case adSmallInt:
				case adInteger:
				case adBigInt:
				case adUnsignedTinyInt:
				case adUnsignedSmallInt:
				case adUnsignedInt:
				case adUnsignedBigInt:
				case adNumeric:
					if (pRecord->GetFieldType(i) == 'I')
					{
						pRecord->Set(i, _v2i(m_ado->rec->GetCollect(_variant_t(p->Name))) );
					}
					else
					{
						if (pRecord->GetFieldType(i) == 'C')
						{
							pRecord->Set(i, _v2s(m_ado->rec->GetCollect(_variant_t(p->Name))) );
						}
						else {
							if (pRecord->GetFieldType(i) == 'R')
								pRecord->Set(i, _v2f(m_ado->rec->GetCollect(_variant_t(p->Name))) );
						}
					}
					break;
				case adBSTR:
				case adChar:
				case adVarChar:
				case adLongVarChar:
				case adWChar:
				case adVarWChar:
				case adLongVarWChar:
				case adBoolean:
					pRecord->Set(i, _v2s(m_ado->rec->GetCollect(_variant_t(p->Name))) );
					break;
				case adDate:	// DATE
				case adDBDate:
				case adDBTime:
				case adDBTimeStamp:
					tm = _v2f(m_ado->rec->GetCollect(_variant_t(p->Name)) );
					SYSTEMTIME ts;
					std::memset(&ts, 0, sizeof(SYSTEMTIME));
					VariantTimeToSystemTime(tm, &ts);
					mDT.year = (BYTE)ts.wYear;
					mDT.month = (BYTE)ts.wMonth;
					mDT.day = (BYTE)ts.wDay;
					mDT.hour = (BYTE)ts.wHour;
					mDT.minute = (BYTE)ts.wMinute;
					mDT.second = (BYTE)ts.wSecond;
					pRecord->Set(i, &mDT);
					break;
				case adDecimal:	// FLOAT
					if (pRecord->GetFieldType(i) == 'R')
					{
						pRecord->Set(i, _v2f(m_ado->rec->GetCollect(_variant_t(p->Name))) );
					}
					else
					{
						pRecord->Set(i, _v2i(m_ado->rec->GetCollect(_variant_t(p->Name))) );
					}
					break;
				case adSingle:	// FLOAT
				case adDouble:	// FLOAT
					pRecord->Set(i, _v2f(m_ado->rec->GetCollect(_variant_t(p->Name))) );
					break;
				}				
			}

			m_ado->rec->MoveNext();		
		}

		m_ado->ResetRecordset();
		m_ado->ResetCommand();
		return true;
	}
	catch (_com_error &e)
	{
		m_ado->ResetRecordset();
		m_ado->ResetCommand();

		std::memset(m_pSql, 0, sizeof(m_pSql));

		PROCESS_ERROR(e);
		
		return false;
	}
	return true;
}

double CSimDB_ADO::GetDouble(int id)
{
	string sField = GetColName(id);
	if (sField.empty())
	{
		CStdString sErr;
		sErr.Format("Exception GetDouble id=%li no this field.", id);
		m_sErr = sErr.c_str();
		m_nCode = -1;
		return 0.F;
	}
	return GetDouble(sField);
}

double CSimDB_ADO::GetDouble(const string &name)
{
	try
	{
		if (m_ado->IsRecordsetNoData())
			return 0.F;
		_variant_t t = m_ado->rec->GetCollect(_variant_t(name.c_str()));
		return _v2f(t);
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);

		CStdString sErr;
		sErr.Format("Exception GetDouble name=%s(%s)", name.c_str(), e.Description());
		m_sErr = sErr.c_str();
		
		return 0.F;
	}
	return 0.F;
}

long CSimDB_ADO::GetLong(int id)
{
	string sField = GetColName(id);
	if (sField.empty())
	{
		CStdString sErr;
		sErr.Format("Exception GetLong id=%li no this field.", id);
		m_sErr = sErr.c_str();
		m_nCode = -1;
		return 0;
	}
	
	return GetLong(sField);
}

long CSimDB_ADO::GetLong(const string &name)
{
	try
	{
		if (m_ado->IsRecordsetNoData())
			return 0;
		_variant_t t = m_ado->rec->GetCollect(_variant_t(name.c_str()));
		return _v2i(t);
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);

		CStdString sErr;
		sErr.Format("Exception GetLong name=%s(%s)", name.c_str(), e.Description());
		m_sErr = sErr.c_str();
		
		return 0;
	}
}

string CSimDB_ADO::Get(int id)
{
	string sField = GetColName(id);
	if (sField.empty())
	{
		CStdString sErr;
		sErr.Format("Exception Get id=%li no this field.", id);
		m_sErr = sErr.c_str();
		m_nCode = -1;
		return string();
	}
	
	return Get(sField);
}

string CSimDB_ADO::Get(const string &name)
{
	try
	{
		if (m_ado->IsRecordsetNoData())
			return string();
		_variant_t t = m_ado->rec->GetCollect(_variant_t(name.c_str()));
		return _v2s(t);
	}
	catch (_com_error &e)
	{		
		PROCESS_ERROR(e);

		CStdString sErr;
		sErr.Format("Exception Get name=%s(%s)", name.c_str(), e.Description());
		m_sErr = sErr.c_str();

		return string();
	}
	return string();
}

bool CSimDB_ADO::MoreForUpdate()
{
	//TODO: 暂时不支持
	m_nCode = -1;
	m_sErr = "暂时不支持.";
	return false;

	m_sErr.clear();

	if (m_nStatus == 0) return false;

	if (!CheckDBConn())
		return false;

	bool bFirst = (m_nStatus == 1);
	if (ExecNoMore() == false)
	{
		return false;
	}
	
	try
	{
		if (m_ado->IsRecordsetNoData())
		{
			return false;
		}

		//TODO: 直接more的用法导致忽略第一条记录,检查这个标记是否合理
		if (!bFirst)
			m_ado->rec->MoveNext();
	
		if (!m_ado->IsRecordsetNoData())
		{
			m_nCount++;
			return true;
		}
	}
	catch (_com_error &e)
	{
		//strcpy( m_cSQL + strlen( m_cSQL ), "\r\n" );
		//UniWriteLog( m_cSQL );
		PROCESS_ERROR(e);
		m_nStatus = 0;
		return false;
	}
	m_nStatus = 0;
	return false;
}

bool CSimDB_ADO::SetBuffer( int nFieldIndex, BYTE *pBuf, long nSize )
{
	//TODO: 暂时不支持
	m_nCode = -1;
	m_sErr = "暂时不支持.";
	return false;

	string sField = GetColName(nFieldIndex);
	if (sField.empty())
	{
		CStdString sErr;
		sErr.Format("Exception SetBuffer id=%li no this field.", nFieldIndex);
		m_sErr = sErr.c_str();
		m_nCode = -1;
		return false;
	}

	return SetBuffer(sField, pBuf, nSize);
}

bool CSimDB_ADO::SetBuffer( const string &sField, BYTE *pBuf, long nSize )
{
	//TODO: 暂时不支持
	m_nCode = -1;
	m_sErr = "暂时不支持.";
	return false;

	if (!CheckDBConn())
		return false;

	try
	{
		if (m_ado->IsRecordsetNoData())
			return false;
		string s((char *)pBuf, nSize);
		m_ado->rec->PutCollect(_variant_t(sField.c_str()), _bstr_t(s.c_str()));
		m_ado->rec->Update();
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);
		return false;
	}
	return true;
}

bool CSimDB_ADO::GetBuffer( const string &sField, BYTE *pBuf, long &nSize )
{
	nSize = 0;
	if (pBuf == NULL)
		return false;

	try
	{
		if (m_ado->IsRecordsetNoData())
			return false;
		_variant_t t = m_ado->rec->GetCollect(_variant_t(sField.c_str()));
		if (t.vt == VT_NULL || t.vt == VT_EMPTY)
			return false;
		
		string s = _bstr_t(t.bstrVal);
		nSize = (long)s.length();
		memcpy(pBuf, s.c_str(), nSize);		
	}
	catch (_com_error &e)
	{
		PROCESS_ERROR(e);

		CStdString sErr;
		sErr.Format("Exception GetBuffer name=%s(%s)", sField.c_str(), e.Description());
		m_sErr = sErr.c_str();

		return false;
	}
	return true;
}

bool CSimDB_ADO::GetBuffer( int nFieldIndex, BYTE *pBuf, long &nSize )
{
	string sField = GetColName(nFieldIndex);
	if (sField.empty())
	{
		CStdString sErr;
		sErr.Format("Exception GetBuffer id=%li no this field.", nFieldIndex);
		m_sErr = sErr.c_str();
		m_nCode = -1;
		return false;
	}
	return GetBuffer(sField, pBuf, nSize);
}

long CSimDB_ADO::GetDataSize( const string &sField )
{
	long id = GetColIndex(sField);
	if (id >= 0)
		return GetDataSize(id);
	else
		return 0;
}

long CSimDB_ADO::GetDataSize( int nFieldIndex )
{
	if (m_ado->IsRecordsetNoData())
		return 0;

	if (nFieldIndex >= 0 && nFieldIndex < m_ado->rec->GetFields()->GetCount())
		return m_ado->rec->GetFields()->GetItem(_variant_t((long)nFieldIndex))->DefinedSize;
	else
		return 0;
}

long CSimDB_ADO::GetActualSize( const string &sField )
{
	long id = GetColIndex(sField);
	if (id >= 0)
		return GetActualSize(id);
	else
		return 0;
}

long CSimDB_ADO::GetActualSize( int nFieldIndex )
{
	if (m_ado->IsRecordsetNoData())
		return 0;

	if (nFieldIndex >= 0 && nFieldIndex < m_ado->rec->GetFields()->GetCount())
		return m_ado->rec->GetFields()->GetItem(_variant_t((long)nFieldIndex))->ActualSize;
	else
		return 0;
}

long CSimDB_ADO::GetColIndex( const string &sField )
{
	if (m_ado->IsRecordsetNoData())
		return -1;

	FieldPtr p;
	long nFieldCount = m_ado->rec->GetFields()->GetCount();

	for (long i = 0; i < nFieldCount; i++)
	{
		p = m_ado->rec->GetFields()->GetItem(_variant_t(i));
		if (strncmp(p->Name, sField.c_str(), sField.length()) == 0)
		{
			return i;
		}
	}

	return -1;
}

string CSimDB_ADO::GetColName( int nFiledIndex )
{
	string rs;

	if (m_ado->IsRecordsetNoData())
		return rs;

	if (nFiledIndex >= 0 && nFiledIndex < m_ado->rec->GetFields()->GetCount())
	{
		return string(m_ado->rec->GetFields()->GetItem(_variant_t((long)nFiledIndex))->Name);
	}
	else
		return rs;
}

string CSimDB_ADO::GetColType( int index )
{
	string sResult;

	if (m_ado->IsRecordsetNoData())
		return sResult;

	if (index < 0 || index >= m_ado->rec->GetFields()->GetCount())
		return sResult;

	long t = m_ado->rec->GetFields()->GetItem(_variant_t((long)index))->Type;
	switch (t)
	{
	case adTinyInt:
	case adSmallInt:
	case adInteger:
	case adBigInt:
	case adUnsignedTinyInt:
	case adUnsignedSmallInt:
	case adUnsignedInt:
	case adUnsignedBigInt:
	case adNumeric:
		sResult = "I";
		break;
	case adBSTR:
	case adChar:
	case adVarChar:
	case adLongVarChar:
	case adWChar:
	case adVarWChar:
	case adLongVarWChar:
	case adBoolean:
		sResult = "C";
		break;
	case adDate:	// DATE
	case adDBDate:
	case adDBTime:
	case adDBTimeStamp:
		sResult = "D";
		break;
	case adDecimal:	// FLOAT
	case adSingle:	// FLOAT  
	case adDouble:	// FLOAT  
		sResult = "R";
		break;
	}

	return sResult;
}

int	CSimDB_ADO::GetNumCols()
{
	if (m_ado->IsRecordsetNoData())
		return 0;
	
	return m_ado->rec->GetFields()->GetCount();
}

string CSimDB_ADO::GetHeadScript()
{
	if (m_ado->IsRecordsetNoData())
		return string();

	long i = 0;
	string sHead;
	CStdString sValue;

	FieldPtr p;
	long nFieldCount = m_ado->rec->GetFields()->GetCount();

	for (i = 0; i < nFieldCount; i++)
	{
		p = m_ado->rec->GetFields()->GetItem(_variant_t(i));
		sValue = "";

		switch (p->GetType())
		{
		case adTinyInt:
		case adSmallInt:
		case adInteger:		
		case adBigInt:
		case adUnsignedTinyInt:
		case adUnsignedSmallInt:
		case adUnsignedInt:
		case adUnsignedBigInt:
		case adNumeric:
			sValue.Format("%s[I]", (char *)p->Name);
			break;
		case adBSTR:
		case adChar:
		case adVarChar:
		case adLongVarChar:
		case adWChar:
		case adVarWChar:
		case adLongVarWChar:
		case adBoolean:
			sValue.Format("%s[C%li]", (char *)p->Name, p->DefinedSize);
			break;
		case adDate:	// DATE
		case adDBDate:
		case adDBTime:
		case adDBTimeStamp:
			sValue.Format("%s[D]", (char *)p->Name);
			break;
		case adDecimal:	// FLOAT
			if (p->NumericScale == 0)			
				sValue.Format("%s[I]", (char *)p->Name);
			else  
				sValue.Format("%s[R]", (char *)p->Name);
			break;
		case adSingle:	// FLOAT  
		case adDouble:	// FLOAT  
			sValue.Format("%s[R]", (char *)p->Name);
			break;
		}
		if (sHead.size() > 0) sHead += ",";
		sHead += sValue;
	}
	return sHead;
}

int CSimDB_ADO::row_getsize()
{
	//SQL Server暂时不支持row_开头的函数
	return 0;
}

bool CSimDB_ADO::row_gethead( string &sHead )
{
	//SQL Server暂时不支持row_开头的函数
	return false;
}

bool CSimDB_ADO::row_more( char *pData, int &nSize )
{
	//SQL Server暂时不支持row_开头的函数
	return false;
}

void CSimDB_ADO::SetAutoCommit( bool autoCommit )
{
	//SQL Server暂时不支持
	//m_bAutoCommit = autoCommit;
	m_bAutoCommit = true;
}

void CSimDB_ADO::BeginTrans()
{
	try
	{
		m_ado->conn->BeginTrans();
	}
	catch (_com_error& e)
	{
		PROCESS_ERROR(e);
	}
}

void CSimDB_ADO::RollBack()
{
	if (!CheckDBConn())
		return;

	try
	{
		m_ado->conn->RollbackTrans();
	}
	catch (_com_error& e)
	{
		PROCESS_ERROR(e);
	}	
}

void CSimDB_ADO::Commit()
{
	if (!CheckDBConn())
		return;

	try
	{
		m_ado->conn->CommitTrans();
	}
	catch (_com_error& e)
	{
		PROCESS_ERROR(e);
	}
}

bool CSimDB_ADO::IsDead()
{
	return (m_ado->conn == NULL || !m_ado->conn->State);
}

char* CSimDB_ADO::GetSql()
{
	return m_pSql;
}

void CSimDB_ADO::ClearSql()
{
	std::memset(m_pSql, 0, sizeof(m_pSql));
}

int CSimDB_ADO::GetCode()
{
	return m_nCode;
}

void CSimDB_ADO::SetValue( int nType, const string &sVal )
{
	m_src = sVal;
}

string CSimDB_ADO::GetValue( int nType )
{
    return  m_src;
}

long CSimDB_ADO::GetCount()
{
	return m_nCount;
}

int CSimDB_ADO::GetDataBaseType()
{
	return m_nDBType;
}

void CSimDB_ADO::SetDBType( int nType )
{
    //m_nDBType = nType;
	m_nDBType = DB_TYPE_SQLSERVER;
}

bool CSimDB_ADO::LoadDB()
{
	if (m_ado->conn == NULL)
		m_ado->conn = _ConnectionPtr(__uuidof(Connection));

	return (m_ado->conn != NULL);
}

string CSimDB_ADO::GetLastErrMsg()
{
    return m_sErr;
}

bool CSimDB_ADO::SetSQLSize( int nSize )
{
	if (nSize <= 0)
		return false;

	if (m_pSql)
	{
		delete[] m_pSql;
		m_pSql = NULL;
	}

	m_pSql = new char[nSize]();
	m_nSqlSize = nSize;
	return true;
}

bool CSimDB_ADO::IsTran()
{
	//TODO: ADO SQL-SERVER 默认自动事务提交
	return true;
}

string CSimDB_ADO::GetSvr()
{
	return m_svr;
}

string CSimDB_ADO::GetUser()
{
	return m_susr;
}

string CSimDB_ADO::GetPwd()
{
	return m_spwd;
}

string CSimDB_ADO::GetDB()
{
	return m_sdb;
}

bool CSimDB_ADO::SetReqBuf( byte *pBuf, int nSize )
{
	m_pReqBuf = pBuf;
	m_nReqBufSize = nSize;
    return true;
}

void CSimDB_ADO::SetReq( void *pInput )
{
	m_pReq = pInput;
}

bool CSimDB_ADO::Reopen( const string &svr, const string &usr, const string &pwd, const string &db)
{
	Close();
	m_bReopen = true;
	return Open(svr, usr, pwd, db);
}

void CSimDB_ADO::SetUserMode(const string &sMode)
{
	m_susermode = sMode;
}

bool CSimDB_ADO::ResetDB()
{
	m_sErr.clear();
	m_nStatus = 1; // sql no exec
	m_nCount = 0;
	m_nCode = 0;

	m_ado->ResetRecordset();
	m_ado->ResetCommand();

	return true;
}

char* CSimDB_ADO::GetSQLPos()
{
	return m_pSql + strlen(m_pSql);
}

void CSimDB_ADO::Proc_Error(_com_error& e, long nLine)
{
	m_nCode = e.WCode();
	m_sErr = e.Description();
	m_bError = true;

	char tmp[100];
	
	CStdString sNote;
	string sTime;
	sNote.Format("[%li %s] ", GetCurDate(), GetCurTime().c_str());
	sTime = sNote.c_str();
	if (m_src.size() > 0)
	{
		sNote += "[Svc:";
		sNote += m_src.c_str();
		sNote += "] ";
	}

	sNote += m_sErr.c_str();
	sNote += " ";

	string sFile = e.Source();
	if (*m_pSql)
	{
		sNote += "[";
		sNote += m_pSql;
		sNote += "] ";
	}
	if (sFile.size() > 0)
	{
		sNote += "[";
		sNote += sFile.c_str();
		char tmp2[50];
		sprintf_s(tmp2, 49, ", %li line] ", nLine);
		sNote += tmp2;
	}
	sprintf_s(tmp, 99, "(Ptr=%li)(Thr=%li)\n", (long)this, ::GetCurrentThreadId());
	sNote += tmp;

	/*
	if( m_pCallStatement )
	{
	sNote += m_pCallStatement->getInputParameter().c_str();
	sNote += "\n";
	}
	*/

	if (m_pReq)
	{
		CSvrComm *pSvrComm = (CSvrComm *)m_pReq;
		if (pSvrComm->m_pBufIn)
		{
			char *p = (char *)pSvrComm->m_pBufIn->GetBuffer();
			TCommHead *pHead = (TCommHead *)pSvrComm->m_pBufIn->GetBuffer();
			p[pHead->nLen] = 0;
			p += sizeof(TCommHead);
			sNote += sTime.c_str();
			sNote += "ReqPacket = [";
			sNote += p;
			sNote += "]\n";
		}
	}
	if (m_ado->conn)
	{
		if (!m_ado->conn->State)
		{
			sNote += sTime.c_str();
			sNote += "用户[" + m_susr + "]与数据库[" + m_svr + "]连接被中断.";
			Close();
			sprintf_s(tmp, 99, "(Ptr=%li)(Thr=%li)\n", (long)this, ::GetCurrentThreadId());
			sNote += tmp;
		}
	}
	else
	{
		if (m_svr.size() > 0)
		{
			sNote += sTime.c_str();
			sNote += "用户[" + m_susr + "]与数据库[" + m_svr + "]连接已处于中断状态.";
			sprintf_s(tmp, 99, "(Ptr=%li)(Thr=%li)\n", (long)this, ::GetCurrentThreadId());
			sNote += tmp;
		}
	}
	UniWriteLog(sNote.c_str());
}

void CSimDB_ADO::WriteSQLLog()
{
	CStdString sNote;
	string sTime;
	sNote.Format("[%li %s] ", GetCurDate(), GetCurTime().c_str());
	sTime = sNote.c_str();
	if (m_src.size() > 0)
	{
		sNote += "[Svc:";
		sNote += m_src.c_str();
		sNote += "] ";
	}
	sNote += m_sErr.c_str();
	sNote += " ";
	if (*m_pSql)
	{
		sNote += "[";
		sNote += m_pSql;
		sNote += "] ";
	}
	sNote += "\n";

	if (m_ado->cmd)
	{
		sNote += m_ado->GetInputParams().c_str();
		sNote += "\n";
	}

	if (m_pReq)
	{
		CSvrComm *pSvrComm = (CSvrComm *)m_pReq;
		if (pSvrComm->m_pBufIn)
		{
			char *p = (char *)pSvrComm->m_pBufIn->GetBuffer();
			TCommHead *pHead = (TCommHead *)pSvrComm->m_pBufIn->GetBuffer();
			p[pHead->nLen] = 0;
			p += sizeof(TCommHead);
			sNote += sTime.c_str();
			sNote += "ReqPacket = [";
			sNote += p;
			sNote += "]\n";
		}
	}
	UniWriteLog(sNote.c_str());
	m_sErr = "";
}

bool CSimDB_ADO::CheckDBConn()
{
	if (m_ado->conn == NULL && m_svr.size() > 0)
	{
		Open(m_svr, m_susr, m_spwd, m_sdb);
	}

	return (m_ado->conn != NULL);
}

bool CSimDB_ADO::IsError()
{
	return m_bError;
}

std::string CSimDB_ADO::GetUserMode()
{
	return m_susermode;
}
void set_bind(DBBINDING &binding, int col, DBBYTEOFFSET len_offset, DBBYTEOFFSET status_offset, DBBYTEOFFSET value_offset, DBLENGTH len, DBTYPE type)
{
	binding.dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
	binding.iOrdinal = col;
	binding.pTypeInfo = NULL;
	binding.obValue = value_offset;
	binding.obLength = len_offset;
	binding.obStatus = status_offset;
	binding.cbMaxLen = len;   // Size of varchar column.
	binding.pTypeInfo = NULL;
	binding.pObject = NULL;
	binding.pBindExt = NULL;
	binding.dwFlags = 0;
	binding.eParamIO = DBPARAMIO_NOTPARAM;
	binding.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
	binding.bPrecision = 10;
	binding.bScale = 255;
	binding.wType = type;
}

HRESULT CSimDB_ADO::fastInsertRow(void *pData)
{
	HRESULT hr = m_pIFastLoad->InsertRow(m_hAccessor, pData);
	return hr;
}

HRESULT CSimDB_ADO::initFastInsert(wstring ip, wstring user, wstring password, wstring tableName)
{
	HRESULT hr = NOERROR;
	CoInitialize(NULL);

	m_pIDataInitialize = NULL; //SQLNCLI_CLSID
	if (!SUCCEEDED(hr = CoCreateInstance(CLSID_MSDAINITIALIZE, NULL, CLSCTX_INPROC_SERVER, IID_IDataInitialize, (void **)&m_pIDataInitialize)))
	{
		return hr;
	}

	m_pIDBInitialize = NULL;
	wstring provider = L"Provider=SQLOLEDB; driver = { sql server }; server=";
	wstring initStringDesc = provider + ip + L"; uid=" + user + L"; pwd=" + password + L";";
	hr = m_pIDataInitialize->GetDataSource(NULL, CLSCTX_INPROC_SERVER, initStringDesc.c_str(), __uuidof(IDBInitialize), (IUnknown**)&m_pIDBInitialize);
	if (FAILED(hr))
	{
		return hr;
	}

	hr = m_pIDBInitialize->Initialize();
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
	DBID TableID;
	TableID.eKind = DBKIND_NAME;
	LPOLESTR x = TableID.uName.pwszName = SysAllocStringLen(tableName.c_str(), tableName.size());

	DBPROP rgProps[1];
	DBPROPSET PropSet;

	VariantInit(&rgProps[0].vValue);

	rgProps[0].dwOptions = DBPROPOPTIONS_OPTIONAL;
	rgProps[0].colid = DB_NULLID;
	rgProps[0].vValue.vt = VT_BOOL;
	rgProps[0].dwPropertyID = SSPROP_ENABLEFASTLOAD;
	rgProps[0].vValue.boolVal = VARIANT_TRUE;

	PropSet.rgProperties = rgProps;
	PropSet.cProperties = 1;
	PropSet.guidPropertySet = DBPROPSET_SQLSERVERROWSET;
	if (FAILED(hr = pIOpenRowsetFL->OpenRowset(NULL, &TableID, NULL, IID_IRowsetFastLoad, 1, &PropSet, (LPUNKNOWN *)&m_pIFastLoad)))
	{
		return hr;
	}
	m_pIFastLoad = m_pIFastLoad;
	IAccessor* pIAccessor = NULL;
	m_hAccessor = 0;
	hr = m_pIFastLoad->QueryInterface(IID_IAccessor, (void **)&pIAccessor);
	if (FAILED(hr))
	{
		return hr;
	}
	IColumnsInfo *pIColumnsInfo = NULL;
	hr = pIAccessor->QueryInterface(IID_IColumnsInfo, (void **)&pIColumnsInfo);
	if (FAILED(hr))
		return hr;
	
	DBORDINAL     pcColumns;
	DBCOLUMNINFO *prgInfo;
	OLECHAR      *pStringsBuffer;
	pIColumnsInfo->GetColumnInfo(&pcColumns, &prgInfo, &pStringsBuffer);
	DBBINDING *rgBindings = NULL;
	rgBindings = (DBBINDING*)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, pcColumns * sizeof(DBBINDING));
	ULONG dwOffset = 0;
	for (ULONG iCol = 0; iCol < pcColumns; iCol++)
	{
		rgBindings[iCol].iOrdinal = prgInfo[iCol].iOrdinal;
		rgBindings[iCol].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
		rgBindings[iCol].obStatus = dwOffset;
		rgBindings[iCol].obLength = dwOffset + sizeof(DBSTATUS);
		rgBindings[iCol].obValue = dwOffset + sizeof(DBSTATUS) + sizeof(ULONG);
		rgBindings[iCol].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
		rgBindings[iCol].eParamIO = DBPARAMIO_NOTPARAM;
		rgBindings[iCol].bPrecision = prgInfo[iCol].bPrecision;
		rgBindings[iCol].bScale = prgInfo[iCol].bScale;
		rgBindings[iCol].wType = prgInfo[iCol].wType;
		rgBindings[iCol].cbMaxLen = prgInfo[iCol].ulColumnSize;
		dwOffset = rgBindings[iCol].cbMaxLen + rgBindings[iCol].obValue;
		dwOffset = ROUND_UP(dwOffset, COLUMN_ALIGNVAL);
	}

	hr = pIAccessor->CreateAccessor(DBACCESSOR_ROWDATA, pcColumns, rgBindings, 0, &m_hAccessor, NULL);
	if (FAILED(hr))
	{
		return hr;
	}
}

HRESULT CSimDB_ADO::fastInsertCommit()
{
	HRESULT hr = m_pIFastLoad->Commit(TRUE);
	return hr;
}

void CSimDB_ADO::finitFastInsert()
{
	CoUninitialize();
	if (m_pIDataInitialize)
	{
		m_pIDataInitialize->Release();
		m_pIDataInitialize = NULL;
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

	if (m_pIFastLoad)
	{
		m_pIFastLoad->Release();
		m_pIFastLoad = NULL;
	}
}

