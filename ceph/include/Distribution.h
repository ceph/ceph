


class Distribution {
  vector p;
  int width;

 public:
  Distribution(int w) { 
	width = w;
  }
  
  int get_width() {
	return width;
  }

  void random() {
	float sum = 0.0;
	for (int i=0; i<width; i++) {
	  p[i] = (float)(rand() % 10000);
	  sum += p[i];
	}
	for (int i=0; i<width; i++) 
	  p[i] /= sum;
  }

  float sample() {
	float s = (float)(rand() % 10000) / 10000.0;
	for (int i=0; i<width; i++) {
	  if (s < p[i]) return i;
	  s -= p[i];
	}
	return width - 1;  // hmm.  :/
  }

  float normalize() {
	float s = 0.0;
	for (int i=0; i<width; i++)
	  s += p[i];
	for (int i=0; i<width; i++)
	  p[i] /= s;
	return s;
  }

};
