SUBDIRS = libhrd hermes mica ccKVS


CLEANDIRS = $(SUBDIRS:%=clean-%)

.PHONY: subdirs clean $(SUBDIRS) $(CLEANDIRS)

subdirs: $(SUBDIRS)
$(SUBDIRS):
	$(MAKE) -C $@

clean: $(CLEANDIRS)
$(CLEANDIRS):
	$(MAKE) -C $(@:clean-%=%) clean
