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

	err = pisn_putsnapshot(point.id, 666, 0, 0);
	if (err)
	{
		printf("pisn_putsnapshot %ld\n", err);
		return err;
	}

	piut_disconnect();
	printf("Success");
	return 0;
}

