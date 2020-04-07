# Coronavirus Opportunity Bot

A Twitter bot that tweets about what an opportunity the Coronavirus / Covid-19 /
SASR-CoV-2 pandemics has been.

![Coronavirus Opportunity Bot](./screenshots/covid-chance.png)

## Installation

### Mac

``` shell
$ brew install python
$ pip install pipenv
$ make setup
```

### Arch Linux

``` shell
# pacman -S pipenv
$ make setup
```

### Other systems

Install these dependencies manually:

- Python >= 3.7
- pipenv

Then run:

``` shell
$ make setup
```

## Usage

1. Create a new directory for the new feed to monitor. Example:

    ``` shell
    $ mkdir -p "data/The Guardian"
    ```

2. Create a text file with the URL of the feed. Example:

    ``` csv
    # data/The Guardian/url.txt
    https://www.theguardian.com/international/rss
    ```

3. Run the processing pipeline with arguments specifying the data directory and
   your Twitter API Access Token. Example

    ``` shell
    $ ./covid-chance \
        --verbose \
        --data-dir="./data" \
        --auth-token="$(secret-tool lookup twitter auth-token)"
    ```

    This will download all articles from the RSS/Atom feed defined in `url.txt`,
    analyze if mention a coronavirus opportunity, create a tweet and post it to
    your Twitter account.

    All intermediate data will be stored in the data directory.

    If the pipeline execution fails anywhere in the process, you can safely
    rerun it and it will continue where it left of.

## Development

### Installation

``` shell
make setup-dev
```

### Testing and linting

``` shell
make test
make lint
```

### Help

``` shell
make help
```

## Contributing

__Feel free to remix this project__ under the terms of the GNU General Public
License version 3 or later. See [COPYING](./COPYING) and [NOTICE](./NOTICE).
