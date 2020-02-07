#include <iostream>
#include <cstdio>
#include "piapix.h"

typedef struct
{
	char* tagname;
	int32 id;
} PT;

int main(int argc, char **argv)
{
	int err;

	if (argc <= 1)
	{
		printf("Missing argument\n");
		printf("Usage: dat2fth PCname pointname\n");
		exit(1);
	}
		
	piut_setprocname("dat2fth");
	printf("Connecting to %s\n", argv[1]);
	err = piut_setservernode(argv[1]);

	if (err)
	{
		printf("piut_setservernode %ld\n", err);
		return err;
	}

	if (argc <= 2)
	{
		printf("Missing argument\n");
		printf("Usage: dat2fth PCname pointname\n");
		exit(1);
	}

	PT point;
	point.tagname = argv[2];

	err = pipt_findpoint(point.tagname, &point.id);
	if (err)
	{
		printf("pipt_findpoint %ld\n", err);
		return err;
	}

	float64 drval = 666;
	int32 istat = 0;
	int16 flags = 0;
	PITIMESTAMP ts;
	pitm_settime(&ts, 2020, 2, 7, 10, 27, 33.333);

	// ptnum,drval,ival,bval,bsize,istat,flags,timestamp
	err = pisn_putsnapshotx(point.id, &drval, NULL, NULL, NULL, &istat, &flags, &ts);
	if (err)
	{
		printf("pisn_putsnapshotx %ld\n", err);
		return err;
	}
	
	piut_disconnect();
	printf("Success");
	return 0;
}

