BUILDTOOLS_ROOT ?= $(abspath ../buildtools)
BUILDTOOLS_ERLANG_VERSION := 15.b.3-dfsg-1~reflex.1-3.1.2-1
BUILDTOOLS_GCC_VERSION := 4.4.7
export LD_LIBRARY_PATH := $(LD_LIBRARY_PATH):$(GCC_SYSROOT)/usr/lib
include $(BUILDTOOLS_ROOT)/make/buildtools.mk

TMP := out/emongo.tmp/
TAR := out/emongo.tar
RPK := out/emongo.rpk

default: all $(RPK)

include out/rxbuild.mk

out:
	mkdir $@

out/rxbuild.mk: rxpackage.json | out
	rxbuild info --make > $@

$(RPK): rxpackage.json $(TAR)
	rxbuild package -r $< $@ $(TAR)

$(TAR):
	rm -rf $(dir $(TMP))
	install -d -m 0755 $(TMP)/ebin
	install -m 0644 ebin/* $(TMP)/ebin/
	mkdir -p $(@D)
	tar -C $(dir $(TMP)) -c . > $@

all: emake $(RPK)

emake:
	erl -make
	@sed -i 's/{ *vsn *,.*}/{vsn, "$(RXBUILD_version)"}/' ebin/emongo.app

test: emake
	prove t/*.t

check: emake
	@ERL_FLAGS="-config app.config" ./rebar eunit skip_deps=true

clean:
	rm -rf $(wildcard ebin/*.beam) erl_crash.dump .eunit/ out/
