_python_pkg := covid_chance
_executable := covid-chance

data_path ?= $(HOME)/.cache/covid-chance/data
config_path ?= $(HOME)/.config/covid-chance/config.json
secrets_path ?= $(HOME)/.config/covid-chance/secrets.json

.PHONY: download-feeds
download-feeds:  ## Download pages from feeds
	"./$(_executable)" python -m "$(_python_pkg).download_feeds" \
		-v --data "$(data_path)" --config "$(config_path)"

.PHONY: download-feed-archives
download-feed-archives:  ## Download feeds from the Internet Archive
	"./$(_executable)" python -m "$(_python_pkg).download_feed_archives" \
		-v --data "$(data_path)" --config "$(config_path)"

.PHONY: download-archived-feeds
download-archived-feeds:  ## Download pages from those feeds that were retrieved from the Internet Archive
	"./$(_executable)" python -m "$(_python_pkg).download_archived_feeds" \
		-v --data "$(data_path)" --config "$(config_path)"

.PHONY: copy-pages-to-db
copy-pages-to-db:  ## Copy downloaded pages into the database
	"./$(_executable)" python -m "$(_python_pkg).copy_pages_to_db" \
		-v --data "$(data_path)" --config "$(config_path)"

.PHONY: match-lines
match-lines:  ## Save lines that match patterns from the pages stored in the database
	"./$(_executable)" python -m "$(_python_pkg).match_lines" \
		--data "$(data_path)" --config "$(config_path)"

.PHONY: parse-lines
parse-lines:  ## Parse matched lines
	"./$(_executable)" python -m "$(_python_pkg).parse_lines" \
		--data "$(data_path)" --config "$(config_path)"

.PHONY: review-tweets
review-tweets:  ## Review created tweets
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" \
		-v --data "$(data_path)" --config "$(config_path)"

.PHONY: post-tweets
post-tweets:  ## Post reviewed tweets
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		-v --data "$(data_path)" --secrets "$(secrets_path)"

.PHONY: post-one-tweet
post-one-tweet:  ## Post a single random tweet
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		-v --data "$(data_path)" --secrets "$(secrets_path)" --one

.PHONY: search-websites
search-websites:  ## Search for website names using DuckDuckGo
	jq --nul-output '.feeds[].name' "$(config_path)" | \
		xargs -0 -I{} xdg-open "https://duckduckgo.com/?q={}"

.PHONY: clean-urls
clean-urls:  ## Clean all URLs in all database tables
	"./$(_executable)" python -m "$(_python_pkg).clean_urls" \
		--data "$(data_path)" --config "$(config_path)"

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

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
