# Coronavirus Opportunity Bot

A Twitter bot that tweets about what an opportunity the Coronavirus / Covid-19 /
SASR-CoV-2 pandemics is.

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

1. Create a configuration file based on
   [config.sample.json](./config.sample.json).

2. Download pages from the news feeds.

    ``` shell
    $ make download
    ```

    Running this command again will always download the latest version of the
    feed and save the new pages. The pages will be stored in the data
    directory. If a page with a given URL is already saved, it will not be
    downloaded again.

3. Create tweets from the downloaded pages:

    ``` shell
    $ make create-tweets
    ```

    The created tweets will be stored in the data directory. Running this
    command again will not recreate existing tweets, it will only create tweets
    for new pages.

4. Review the created tweets:

    ``` shell
    $ make review-tweets
    ```

    The reviewed tweets will be stored in a file in the data directory. Running
    this command again will not ask you for a review of the same tweets again.

5. Create a file with you Twitter API secrets based on
   [secrets.sample.json](./secrets.sample.json).

6. Post the reviewed tweets:

    ``` shell
    $ make post-tweets
    ```

    The posted tweets will be stored in a file in the data directory. Running
    this command again will not post the same tweets again.

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
