#include <iostream>
#include <string>
using namespace std;

int make_dirs(const char *basedir, int dirs, int files, int depth)
{
  //if (time_to_stop()) return 0;

  // make sure base dir exists
  int r = mkdir(basedir, 0755);
  if (r != 0) {
    cout << "can't make base dir? " << basedir << endl;
    return -1;
  }

  // children
  char d[500];
  cout << "make_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << endl;
  for (int i=0; i<files; i++) {
    sprintf(d,"%s/file.%d", basedir, i);
    mknod(d, 0644);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
    sprintf(d, "%s/dir.%d", basedir, i);
    make_dirs(d, dirs, files, depth-1);
  }
  
  return 0;
}

int main()
{
  make_dirs("blah", 10, 10, 4);

}
