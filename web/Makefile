
all: index.html overview.html publications.html source.html

%.html: %.body template.html
	./gen.pl $< > $@
