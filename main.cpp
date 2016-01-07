#include "StdAfx.h"
#include <iostream>

#include "SimDB_ado.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <comutil.h>
//#include "msado15.tlh"

void test_insert(CSimDB_ADO& db)
{
	printf("-----insert-------\n");
	//db.BeginTrans();
	printf("exec: err=%d, %s \n", db.GetCode(), db.GetLastErrMsg().c_str());
	db.Cmd("insert into [user](id, name, info, cost)values(?, ?, ?, ?)");
	db.Set("id", (long)13);
	db.Set("name", "name7");
	db.Set("info", "info4");
	db.Set("cost", 6.9);
	//db.SetNull("cost");
	long rs = db.Exec();
	printf("exec: %d, err=%d, %s \n", rs, db.GetCode(), db.GetLastErrMsg().c_str());
}

void test_select(CSimDB_ADO& db)
{
	printf("-----select-------\n");

	long id;
	double cost;
	string sname, sinfo;

	db.Cmd("select * from [user];");
	char buf[50] = { 0 };
	long nlen = 0;
	while (db.More())
	{
		id = db.GetLong(0);
		sname = db.Get(1);
		//sinfo = db.Get("info");
		memset(buf, 0, 50);
		db.GetBuffer("info", (BYTE *)buf, nlen);
		sinfo = string(buf, nlen);
		cost = db.GetDouble("cost");
		printf("id=%d, name=%s, info=%s, cost=%f \n", id, sname.c_str(), sinfo.c_str(), cost);
	}

	printf("exec: err=%d, %s \n", db.GetCode(), db.GetLastErrMsg().c_str());
}

void test_selectinfo(CSimDB_ADO& db)
{
	printf("----- selectinfo save file -------\n");

	db.Cmd("select * from [user];");
	CStdRecord rec;
	db.SelectInto(&rec, true);
	bool rs = rec.SaveToFile("t:\\user.log");
	printf(rs ? "save ok. \n" : "save failed. \n");
}

void test_max_sqlsize(CSimDB_ADO& db)
{
	long maxlen = 10 * 1024 * 1024 + 1;
	char* buf = new char[maxlen];
	FILE* f = fopen("t:/insert.sql", "r");
	long len = fread(buf, 1, maxlen, f);
	buf[len] = 0;
	fclose(f);
	db.SetSQLSize(len + 1);
	db.Cmd(buf);
	//db.BeginTrans();
	long rs = db.Exec();
	//db.Commit();
	printf("exec: %d, err=%d, %s \n", rs, db.GetCode(), db.GetLastErrMsg().c_str());
}
struct DataRow
{
	int id;
};
int main()
{
 	CSimDB_ADO db;
	HRESULT hr;
	hr = db.initFastInsert(L"192.168.133.139", L"sa", L"abc**123",L"test", L"[test].[dbo].[tid]");
	if (FAILED(hr))
	{
		cout << "initFastInsert failed, " << hr << endl;
		system("pause");
		return -1;
	}
	DataRow oneRow;
	for (int i = 0; i < 10; i++)
	{
		oneRow.id = i;
		db.fastInsertRow((void *)&oneRow);
	}
	db.fastInsertCommit();
	db.finitFastInsert();
	system("pause");
	getchar();
// 	if (db.Open("192.168.133.136", "sa", "abc**123", "test"))
// 	{
// 		printf("open database ok! \n");
// 
// 		test_insert(db);
// 
// 		//test_select(db);
// 
// 		//test_selectinfo(db);
// 
// 		//test_max_sqlsize(db);
// 
// 		db.Close();
// 		printf("close database ok! \n");
// 	}
// 	else
// 	{
// 		printf("open database failed! \n");
// 		printf("error: %d, %s \n", db.GetCode(), db.GetLastErrMsg().c_str());
// 	}
// 	getchar();
// 	CoUninitialize();
	return 0;
}