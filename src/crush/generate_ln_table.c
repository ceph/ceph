#include <stdio.h>
#include <math.h>

unsigned fake_ln(unsigned x)
{
	double ln = log((double)(x+1) / (double)0x10000);
	ln += 11.090355;
	unsigned y = ln * (double)0x10000 / 11.090355;
	if (y > 0xffff)
		y = 0xffff;
	return y;
}

int main(int argc, char **argv)
{
	int i;
	double min = -11.090355;
	double max = 0;
	int target_max = 0x10000;
	printf("static uint16_t crush_ln_table[65536] = {");
	for (i = 0; i < 0x10000; ++i) {
		double ln = log((double)(i+1) / (double)0x10000);
		double ln2 = (ln - min);
		unsigned x = (ln2) * (target_max / (max - min));
		//printf("%x\t%lf\t%lf\t%x\t%x\n", i, ln, ln2, x, fake_ln(i));
		if (i % 10 == 0) {
			if (i)
				printf(",");
			printf("\n\t");
		} else {
			printf(",");
		}
		printf("0x%x", fake_ln(i));
	}
	printf("\n};\n");
}
