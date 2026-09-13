# Makefile for swatch

AUTOMAKE_OPTIONS = foreign

AM_CPPFLAGS = -I$(top_srcdir)

bin_PROGRAMS = swatch

swatch_LDADD = $(LIB_SLURM)
swatch_DEPENDENCIES = $(LIB_SLURM_BUILD)

noinst_HEADERS  = opt.h
swatch_SOURCES = swatch.c opt.c

swatch_LDFLAGS = $(CMD_LDFLAGS)

REF = help.txt usage.txt
include $(top_srcdir)/make_ref.include
noinst_LTLIBRARIES = $(LIB_REF)
EXTRA_swatch_DEPENDENCIES = $(LIB_REF)
swatch_LDADD += $(LIB_REF)

force:
$(swatch_DEPENDENCIES) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
