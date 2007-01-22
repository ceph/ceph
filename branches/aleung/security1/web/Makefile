
all: index.html overview.html publications.html source.html tasks.html

%.html: %.body template.html
	./gen.pl $< > $@
