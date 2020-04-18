_python_pkg := covid_chance
_executable := covid-chance

data_path ?= $(HOME)/.cache/covid-chance/data
config_path ?= $(HOME)/.config/covid-chance/config.json
secrets_path ?= $(HOME)/.config/covid-chance/secrets.json

.PHONY: download-feeds download-feed-archives download-archived-feeds copy-pages-to-db create-tweets review-tweets post-tweets post-one-tweet search-websites setup setup-dev test lint tox reformat help

download-feeds:  ## Download pages from feeds
	"./$(_executable)" python -m "$(_python_pkg).download_feeds" \
		-v --data "$(data_path)" --config "$(config_path)"

download-feed-archives:  ## Download feeds from the Internet Archive
	"./$(_executable)" python -m "$(_python_pkg).download_feed_archives" \
		-v --data "$(data_path)" --config "$(config_path)"

download-archived-feeds:  ## Download pages from those feeds that were retrieved from the Internet Archive
	"./$(_executable)" python -m "$(_python_pkg).download_archived_feeds" \
		-v --data "$(data_path)" --config "$(config_path)"

copy-pages-to-db:  ## Copy downloaded pages into the database
	"./$(_executable)" python -m "$(_python_pkg).copy_pages_to_db" \
		-v --data "$(data_path)" --config "$(config_path)"

create-tweets:  ## Create tweets from the pages stored in the database
	"./$(_executable)" python -m "$(_python_pkg).create_tweets" \
		--data "$(data_path)" --config "$(config_path)"

review-tweets:  ## Review created tweets
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" \
		-v --data "$(data_path)" --config "$(config_path)"

post-tweets:  ## Post reviewed tweets
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		-v --data "$(data_path)" --secrets "$(secrets_path)"

post-one-tweet:  ## Post a single random tweet
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		-v --data "$(data_path)" --secrets "$(secrets_path)" --one

search-websites:  ## Search for website names using DuckDuckGo
	jq --nul-output '.feeds[].name' "$(config_path)" | \
		xargs -0 -I{} xdg-open "https://duckduckgo.com/?q={}"

setup:  ## Create Pipenv virtual environment and install dependencies.
	pipenv --three --site-packages
	pipenv install

setup-dev:  ## Install development dependencies
	pipenv install --dev

test:  ## Run unit tests
	pipenv run python -m unittest

lint:  ## Run linting
	pipenv run flake8 $(_python_pkg)
	pipenv run mypy $(_python_pkg) --ignore-missing-imports
	pipenv run isort -c -rc $(_python_pkg)

tox:  ## Test with tox
	tox -r

reformat:  ## Reformat Python code using Black
	black -l 79 --skip-string-normalization $(_python_pkg)
	pipenv run isort -rc $(_python_pkg)

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
