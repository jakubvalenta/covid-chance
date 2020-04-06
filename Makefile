_python_pkg = coronavirus_opportunity_bot
_executable = coronavirus-opportunity-bot
_executable_clean = coronavirus-opportunity-bot-clean

.PHONY: download create-tweets post-tweets clean-tweets setup setup-dev test lint tox reformat help

download:  ## Download pages from feeds
	"./$(_executable)" luigi --module "$(_python_pkg).download_feeds" \
		DownloadFeeds \
		--verbose \
		--workers 2 --local-scheduler --log-level INFO

create-tweets:  ## Create tweets from downloaded pages
	"./$(_executable)" luigi --module "$(_python_pkg).create_tweets" \
		CreateTweets \
		--verbose \
		--keywords '["opportunity"]' \
		--pattern 'opportunity to (?P<parsed>.+?)([\.?!;]|( \|)|$$)' \
		--template '$${parsed} #Covid_19 @$${handle} $${url}' \
		--workers 2 --local-scheduler --log-level INFO

review-tweets:  ## Review created tweets
	"./$(_executable)" python -m "$(_python_pkg).review_tweets" --verbose

post-tweets:  ## Post reviewed tweets
	"./$(_executable)" python -m "$(_python_pkg).post_tweets" --verbose

clean-tweets:  ## Remove created tweets
	"./$(_executable)" python -m "$(_python_pkg).clean_tweets" --verbose

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

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
