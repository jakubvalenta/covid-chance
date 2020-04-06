_python_pkg = coronavirus_opportunity_bot
_executable = coronavirus-opportunity-bot
_executable_clean = coronavirus-opportunity-bot-clean

.PHONY: run clean clean-analysis clean-joined setup setup-dev test lint tox reformat help

run:  ## Run the pipeline
	"./$(_executable)" \
		--verbose \
		--keywords '["opportunity"]' \
		--pattern 'opportunity to (?P<parsed>.+?)([\.?!;]|( \|)|$$)' \
		--template '$${parsed} #Covid_19 $${url}' \
		$(args) --workers 2 --local-scheduler --log-level WARNING

clean:  ## Remove all intermediate files except downloaded HTML pages
	"./$(_executable_clean)" $(args)

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
