_python_pkg := covid_chance
_dir := $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
_cmd := $(_dir)/covid-chance -v --config "$(config_path)"

cache_path ?= $(HOME)/.cache/covid-chance
config_path ?= $(HOME)/.config/covid-chance/config.json
secrets_path ?= $(HOME)/.config/covid-chance/secrets.json
print_export_dir ?= print

.PHONY: download-feeds
download-feeds:  ## Download current feeds
	$(_cmd) download-feeds

.PHONY: download-archives
download-archives:  ## Save URLs of feeds archived by the Internet Archive
	$(_cmd) download-archives

.PHONY: download-feeds-from-archives
download-feeds-from-archives:  ## Download feeds from the saved Internet Archive URLs
	$(_cmd) download-feeds-from-archives --cache "$(cache_path)"

.PHONY: download-pages
download-pages:  ## Download pages for all downloaded feeds
	$(_cmd) download-pages --cache "$(cache_path)"

.PHONY: match-lines
match-lines:  ## Save lines that match patterns from the pages stored in the database
	$(_cmd) match-lines

.PHONY: parse-lines
parse-lines:  ## Parse matched lines
	$(_cmd) parse-lines

.PHONY: review-tweets
review-tweets:  ## Review created tweets
	$(_cmd) review-tweets

.PHONY: review-tweets-all
review-tweets-all:  ## Review all tweets again
	$(_cmd) review-tweets --all

.PHONY: review-tweets-approved
review-tweets-approved:  ## Review approved tweets again
	$(_cmd) review-tweets --approved

.PHONY: post-tweet
post-tweet:  ## Post one random reviewed tweet
	$(_cmd) post-tweet --secrets "$(secrets_path)"

.PHONY: post-tweet-interactive
post-tweet-interactive:  ## Post one random reviewed tweet (ask before posting)
	$(_cmd) post-tweet --secrets "$(secrets_path)" --interactive

.PHONY: check-posted-tweets
check-posted-tweets:  ## Check posted tweets
	$(_cmd) check-posted-tweets --secrets "$(secrets_path)"

.PHONY: show-stats
show-stats:  ## Show statistics
	$(_cmd) show-stats

.PHONY: migrate
migrate:  ## Migrate old database
	$(_cmd) migrate

.PHONY: print-export
print-export:  ## Print export
	$(_cmd) print-export --cache "$(cache_path)"

.PHONY: print-format
print-format:  ## Print format
	$(_cmd) print-format --output "$(print_export_dir)"

.PHONY: search-websites
search-websites:  ## Search for website names using DuckDuckGo
	jq --nul-output '.feeds[].name' "$(config_path)" | \
		xargs -0 -I{} xdg-open "https://duckduckgo.com/?q={}"

.PHONY: setup
setup:  ## Create Pipenv virtual environment and install dependencies.
	pipenv --three --site-packages
	pipenv install

.PHONY: setup-dev
setup-dev:  ## Install development dependencies
	pipenv install --dev

.PHONY: test
test:  ## Run unit tests
	pipenv run python -m unittest

.PHONY: lint
lint:  ## Run linting
	pipenv run flake8 $(_python_pkg)
	pipenv run mypy $(_python_pkg) --ignore-missing-imports
	pipenv run isort -c $(_python_pkg)

.PHONY: tox
tox:  ## Test with tox
	tox -r

.PHONY: reformat
reformat:  ## Reformat Python code using Black
	black -l 79 --skip-string-normalization $(_python_pkg)
	pipenv run isort -rc $(_python_pkg)

.PHONY: python-shell
python-shell:  ## Run Python shell with all dependencies installed
	pipenv run ipython --no-banner --no-confirm-exit

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
