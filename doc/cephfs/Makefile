TARGETS=mds-state-diagram.svg

%.svg: %.dot
	dot -Tsvg -o $@ $^


all: $(TARGETS)
