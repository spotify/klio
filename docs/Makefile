SPHINXOPTS      ?=
SPHINXBUILD     ?= sphinx-build
SPHINXAUTOBUILD ?= sphinx-autobuild
SOURCEDIR       = src
BUILDDIR        = build
PORT            := 8888
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(SPHINXOPTS) $(SOURCEDIR)
# -n for nitpick, -T for show full traceback on exception, -W to turn warnings to errors
STRICTOPTS      = -n -T -W


# Put it first so that "make" without argument is like "make help".
help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html        to make standalone HTML files"
	@echo "  linkcheck   to check all external links for integrity"
	@echo "  spellcheck  to spell check documentation"
	@echo "  livehtml    to build and serve HTML (reloads on changes)"
	@echo "  stricthtml  to build HTML files in 'strict' mode"
	@echo "  clean       to clean build directory"

.PHONY: help clean html livehtml linkcheck stricthtml spellcheck


clean:
	rm -rf $(BUILDDIR)/*

html:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html"

stricthtml: clean
	$(SPHINXBUILD) $(STRICTOPTS) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished in strict mode. The HTML pages are in $(BUILDDIR)/html"

linkcheck:
# 	Temporary workaround for https://gitlab.com/documatt/sphinx-reredirects/-/merge_requests/1
# 	$(SPHINXBUILD) -q -b linkcheck $(ALLSPHINXOPTS) $(BUILDDIR)/linkcheck
	@bash linkcheck_fix.sh
	@echo
	@echo "Link check complete; look for any errors in the above output " \
	      "or in $(BUILDDIR)/linkcheck/output.txt."

spellcheck:
	$(SPHINXBUILD) -b spelling -W $(ALLSPHINXOPTS) $(BUILDDIR)/spellcheck
	@echo
	@echo "Spell check complete; look for any errors in the above output " \
	      "or in $(BUILDDIR)/spellcheck/."

# sphinx-autobuild is required
livehtml:
	$(SPHINXAUTOBUILD) -p $(PORT) -b html $(SOURCEDIR) $(BUILDDIR)/html $(O)
