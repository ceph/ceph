
#include "../../common/Clock.h"
#include "../crush.h"
using namespace crush;


Clock g_clock;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


int branching = 10;
bool linear = false;
int numrep = 1;

int main() {

  Bucket *b = new UniformBucket(1, 0);
  //b = new TreeBucket(1);
}

