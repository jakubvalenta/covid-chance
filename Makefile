_python_pkg := covid_chance
_executable := covid-chance
_executable_clean := covid-chance-clean

data_path := $(HOME)/.cache/covid-chance/data
config_path := $(HOME)/.config/covid-chance/config.json
secrets_path := $(HOME)/.config/covid-chance/secrets.json

.PHONY: download-feeds download-archived-feeds create-tweets post-tweets post-one-tweet clean-tweets clean-data search-websites setup setup-dev test lint tox reformat help

download-feeds:  ## Download pages from feeds
	"./$(_executable)" python -m "$(_python_pkg).download_feeds" \
		-v --data "$(data_path)" --config "$(config_path)"

download-archived-feeds:  ## Download pages from those feeds that were retrieved from the Internet Archive
	"./$(_executable)" python -m "$(_python_pkg).download_archived_feeds" \
		-v --data "$(data_path)" --config "$(config_path)"

create-tweets:  ## Create tweets from downloaded pages
	"./$(_executable)" python -m "$(_python_pkg).create_tweets" \
		-v --data "$(data_path)" --config "$(config_path)"

review-tweets:  ## Review created tweets
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" \
		-v --data "$(data_path)" --config "$(config_path)"

post-tweets:  ## Post reviewed tweets
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		-v --data "$(data_path)" --secrets "$(secrets_path)"

post-one-tweet:  ## Post a single random tweet
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		-v --data "$(data_path)" --secrets "$(secrets_path)" --one

clean-tweets:  ## Remove created tweets
	"./$(_executable)" python -m "$(_python_pkg).clean_tweets" \
		-v --data "$(data_path)" --config "$(config_path)"

clean-data:  ## Rename data files to match current naming scheme
	"./$(_executable)" python -m "$(_python_pkg).clean_data" \
		-v --data "$(data_path)" --config "$(config_path)"

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
