_python_pkg := covid_chance
_executable := covid-chance

cache_path ?= $(HOME)/.cache/covid-chance
config_path ?= $(HOME)/.config/covid-chance/config.json
secrets_path ?= $(HOME)/.config/covid-chance/secrets.json

.PHONY: download-feeds
download-feeds:  ## Download current feeds
	"./$(_executable)" python -m "$(_python_pkg).download_feeds" -v --config "$(config_path)"

.PHONY: download-archives
download-archives:  ## Save URLs of feeds archived by the Internet Archive
	"./$(_executable)" python -m "$(_python_pkg).download_archives" -v --config "$(config_path)"

.PHONY: download-feeds-from-archives
download-feeds-from-archives:  ## Download feeds from the saved Internet Archive URLs
	"./$(_executable)" python -m "$(_python_pkg).download_feeds_from_archives" -v --config "$(config_path)" \
		--cache "$(cache_path)"

.PHONY: download-pages
download-pages:  ## Download pages for all downloaded feeds
	"./$(_executable)" python -m "$(_python_pkg).download_pages" -v --config "$(config_path)" \
		--cache "$(cache_path)"

.PHONY: match-lines
match-lines:  ## Save lines that match patterns from the pages stored in the database
	"./$(_executable)" python -m "$(_python_pkg).match_lines" -v --config "$(config_path)"

.PHONY: parse-lines
parse-lines:  ## Parse matched lines
	"./$(_executable)" python -m "$(_python_pkg).parse_lines" -v --config "$(config_path)"

.PHONY: review-tweets
review-tweets:  ## Review created tweets
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" -v --config "$(config_path)"

.PHONY: review-tweets-all
review-tweets-all:  ## Review all tweets again
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" -v --config "$(config_path)" \
		--all

.PHONY: review-tweets-approved
review-tweets-approved:  ## Review approved tweets again
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" -v --config "$(config_path)" \
		--approved

.PHONY: post-tweet
post-tweet:  ## Post one random reviewed tweet
	"./$(_executable)" python -m "$(_python_pkg).post_tweet" -v --config "$(config_path)" \
		--secrets "$(secrets_path)"

.PHONY: post-tweet-interactive
post-tweet-interactive:  ## Post one random reviewed tweet (ask before posting)
	"./$(_executable)" python -m "$(_python_pkg).post_tweet" -v --config "$(config_path)" \
		--secrets "$(secrets_path)" --interactive

.PHONY: print-stats
print-stats:  ## Print statistics
	"./$(_executable)" python -m "$(_python_pkg).print_stats" -v --config "$(config_path)"

.PHONY: prepare-print-export
prepare-print-export:  ## Prepare print export
	"./$(_executable)" python -m "$(_python_pkg).prepare_print_export" -v --config "$(config_path)" \
		--cache "$(cache_path)"

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
	pipenv run isort -c -rc $(_python_pkg)

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
