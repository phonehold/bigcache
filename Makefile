all:
	tsxs -o bgcache.so bgcache.cc

clean:
	rm -rf *.so *.o
