# contrib/file_fdw/Makefile

MODULES = file_textarray_fdw

EXTENSION = file_textarray_fdw
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | \
	sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

DATA_built = $(EXTENSION)--$(EXTVERSION).sql

REGRESS = file_textarray_fdw

all: $(EXTENSION)--$(EXTVERSION).sql

$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
	cp $< $@

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/file_textarray_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
