AUTOMAKE_OPTIONS = foreign subdir-objects

AM_CPPFLAGS = -I$(top_srcdir)
LDADD = -ldl -lpthread -L$(top_builddir)/src/api/.libs -lslurmfull

check_PROGRAMS = \
	$(TESTS)

if HAVE_CHECK
MYCFLAGS  = @CHECK_CFLAGS@ -Wall

if CLANG
MYCFLAGS += -Xclang -pic-level -Xclang 0
endif

TESTS = job_subs-test

job_subs_test_SOURCES = job_subs-test.c \
			$(top_srcdir)/src/slurmctld/job_subs.c	\
			$(top_srcdir)/src/slurmctld/job_subs.h	\
			$(top_srcdir)/src/slurmctld/locks.c

job_subs_test_CFLAGS = $(MYCFLAGS)
job_subs_test_LDADD  = $(LDADD) @CHECK_LIBS@
job_subs_test_LDFLAGS = -z lazy  -Wl,--unresolved-symbols=ignore-all -Wl,--allow-multiple-definition  -Wl,-rpath=$(top_builddir)/src/api/.libs -export-dynamic

endif
