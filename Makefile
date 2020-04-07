_python_pkg = covid_chance
_executable = covid-chance
_executable_clean = covid-chance-clean

.PHONY: download create-tweets post-tweets clean-tweets setup setup-dev test lint tox reformat help

download:  ## Download pages from feeds
	"./$(_executable)" python -m "$(_python_pkg).download_feeds" \
		--verbose \
		--data-path "$(HOME)/.cache/covid-chance/data"

create-tweets:  ## Create tweets from downloaded pages
	"./$(_executable)" python -m "$(_python_pkg).create_tweets" \
		--verbose \
		--data-path "$(HOME)/.cache/covid-chance/data" \
		--config "$(HOME)/.config/covid-chance/config.json"

review-tweets:  ## Review created tweets
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" \
		--verbose \
		--data-path "$(HOME)/.cache/covid-chance/data" \

post-tweets:  ## Post reviewed tweets
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		--verbose \
		--data-path "$(HOME)/.cache/covid-chance/data" \

post-single-tweet:  ## Post a single random tweet
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" \
		--verbose \
		--single \
		--data-path "$(HOME)/.cache/covid-chance/data" \

clean-tweets:  ## Remove created tweets
	"./$(_executable)" python -m "$(_python_pkg).clean_tweets" \
		--verbose \
		--data-path "$(HOME)/.cache/covid-chance/data" \

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
