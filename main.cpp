#include "StdAfx.h"
#include <iostream>

#include "SimDB_ado.h"

#include <time.h>
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <comutil.h>
//#include "msado15.tlh"

#define COUNT 10

struct MyData
{
	int rec_num;
	char date[9];
	char time[9];
	char reff[11];
	double cost;
	char acc[11];
	char stock[7];
	char bs[2];
	char price[9];
	char qty[9];
	char status[2];
	char owflag[4];
	char ordrec[9];
	char firmid[6];
	char branchid[6];
	char checkord[16];
};
int main()
{
 	CSimDB_ADO db;
	HRESULT hr;
	hr = db.initQuickInsert(L"192.168.133.151", L"sa", L"abc**123",L"test", L"order");
	if (FAILED(hr))
	{
		cout << "initFastInsert failed, " << hr << endl;
		system("pause");
		return -1;
	} 
	time_t begin;
	time(&begin);

	MyData oneRow[5];

	for (int j = 0; j < 100000; j++)
	{
		for (int i = 0; i < 5; i++)
		{
			oneRow[i].rec_num = i;
			strcpy_s(oneRow[i].date, "201601");
			strcpy_s(oneRow[i].time, "20150121");
			strcpy_s(oneRow[i].reff, "1234567890");
			oneRow[i].cost = 1.003;
			strcpy_s(oneRow[i].acc, "0000011111");
			strcpy_s(oneRow[i].stock, "123456");
			strcpy_s(oneRow[i].bs, "B");
			strcpy_s(oneRow[i].price, "1.000");
			strcpy_s(oneRow[i].qty, "1000");
			strcpy_s(oneRow[i].status, "R");
			strcpy_s(oneRow[i].owflag, "ORD");
			strcpy_s(oneRow[i].ordrec, "1");
			strcpy_s(oneRow[i].firmid, "123");
			strcpy_s(oneRow[i].branchid, "20201");
			strcpy_s(oneRow[i].checkord, "20201");
			//memset(oneRow[i].checkord, 0, 16);
			hr = db.quickInsert((void *)&oneRow[i]);
			if (FAILED(hr))
			{
				cout << "initFastInsert failed, " << hr <<"   i = " << i<< endl;
				break;
			}
		}
	}

	if (SUCCEEDED(hr))
	{
		hr = db.quickInsertCommit();
		if (FAILED(hr))
		{
			cout << "data insert commit failed!" << endl;
		}
		else
		{
			cout << "data insert commit success!" << endl;
		}

	}
	else
	{
		cout << "data insert failed!" << endl;
	}
	db.finitQuickInsert();
	time_t end;
	time(&end);
	printf("Elapse Time= [%lld]\n", end - begin);
	
	system("pause");
	getchar();
	return 0;
}