BUILDTOOLS_ROOT ?= $(abspath ../buildtools)

include $(BUILDTOOLS_ROOT)/make/defaults.mk

export LD_LIBRARY_PATH := $(LD_LIBRARY_PATH):$(GCC_SYSROOT)/usr/lib
include $(BUILDTOOLS_ROOT)/make/buildtools.mk

TMP := out/emongo.tmp/
TAR := out/emongo.tar
APK := out/emongo.apk

default: all $(APK)

include out/abuild.mk

TMP := out/emongo.tmp/emongo-$(ABUILD_version)

out:
	mkdir $@

out/abuild.mk: apackage.json | out
	abuild info --make > $@

$(APK): apackage.json $(TAR)
	abuild package -r $< $@ $(TAR)

$(TAR): Makefile emake
	rm -rf $(dir $(TMP))
	install -d -m 0755 $(TMP)/ebin
	install -m 0644 ebin/* $(TMP)/ebin/
	mkdir -p $(@D)
	tar -C $(dir $(TMP)) -c . > $@

all: emake $(APK)

emake:
	erl -make
	@sed -i 's/{ *vsn *,.*}/{vsn, "$(ABUILD_version)"}/' ebin/emongo.app

test: emake
	prove t/*.t

check: emake
	@ERL_FLAGS="-config app.config" ./rebar eunit skip_deps=true

clean:
	rm -rf $(wildcard ebin/*.beam) erl_crash.dump .eunit/ out/
