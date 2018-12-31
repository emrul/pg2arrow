PG_CONFIG := pg_config
PROGRAM    = pg2arrow

OBJS = pg2arrow.o buffer.o
PG_CPPFLAGS = -I/usr/include/arrow-glib \
              -I/usr/include/glib-2.0   \
              -I/usr/lib64/glib-2.0/include \
              -I$(shell $(PG_CONFIG) --includedir)
PG_LIBS = -larrow-glib -lpq

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
