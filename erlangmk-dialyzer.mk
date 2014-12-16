# Copyright (c) 2013-2014, Loïc Hoguin <essen@ninenines.eu>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# Copyright (c) 2013-2014, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.


#To use, first build a plt with 'make plt'
#Then run Dialyzer with 'make dialyze APPS="app1 app2 app3"', for any combination of 
#apps in ceres you wish to run.



.PHONY: plt distclean-plt dialyze

# Configuration.

#cphi @2014 - Modified this line so it will always apply
DIALYZER_PLT = $(CURDIR)/.$(PROJECT).plt
export DIALYZER_PLT

EXCLUDE=deps/vierl/ebin
PLT_APPS ?= $(filter-out $(EXCLUDE), $(wildcard deps/*/ebin))



DIALYZER_DIRS ?= ebin
DIALYZER_OPTS ?= -Werror_handling -Wrace_conditions \
	# -Wunmatched_returns  -Wunderspecs

# Core targets.

distclean:: distclean-plt

help::
	@printf "%s\n" "" \
		"Dialyzer targets:" \
		"  plt         Build a PLT file for this project" \
		"  dialyze     Analyze the project using Dialyzer"

# Plugin-specific targets.

$(DIALYZER_PLT):  
	@dialyzer --build_plt --apps erts kernel stdlib xmerl inets ssl mnesia crypto ssh sasl eldap public_key rabbitmq_server $(PLT_APPS)

plt: $(DIALYZER_PLT) 

distclean-plt:
	$(gen_verbose) rm -f $(DIALYZER_PLT)

ifneq ($(wildcard $(DIALYZER_PLT)),)
dialyze:
else
dialyze: $(DIALYZER_PLT)
endif
	@dialyzer --no_native $(DIALYZER_DIRS) $(DIALYZER_OPTS)