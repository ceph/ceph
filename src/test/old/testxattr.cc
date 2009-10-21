
#include <iostream>
using namespace std;


#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <errno.h>
#include <dirent.h>
#include <sys/xattr.h>

int main(int argc, char**argv)
{
  int a = 1;
  int b = 2;

  mknod("test", 0600, 0);

  cout << "setxattr " << setxattr("test", "asdf", &a, sizeof(a), 0) << endl;
  cout << "errno " << errno << " " << strerror(errno) << endl;
  cout << "getxattr " << getxattr("test", "asdf", &b, sizeof(b)) << endl;
  cout << "errno " << errno << " " << strerror(errno) << endl;
  cout << "a is " << a << " and b is " << b << endl;
  return 0;
}
