# Covid-19 is...

A Twitter account that tweets about all the chances and opportunities Covid-19
gives us. [@covid_chance](https://twitter.com/covid_chance)

![Coronavirus Opportunity Bot](./screenshots/covid-chance.png)

This repository contains a general tool to search the web for specific
expressions and then review and post tweets created from the found text.

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

This program works in the following steps:

1. **Download RSS/Atom feeds** and store the URLs found in those feeds in the
   database.
2. **Download the HTML pages** from the URLs stored in the database, convert
   them to plain text and store the text.
3. **Search the text content** of the pages for specified keywords and store the
   text paragraphs that match.
4. **Create tweets** from the stored paragraphs by applying a specific
   formatting pattern.
5. **Manually review** the created tweets using an interactive command-line interface.
6. **Post the tweets** to Twitter.

### Database

This program requires a running PostgreSQL with an existing database. Therefore
you need to install PostgreSQL and create a database before running the program.

Example for Arch Linux:

``` shell
# pacman -S postgresql
$ sudo -iu postgres
[postgres]$ initdb -D /var/lib/postgres/data
[postgres]$ createuser --interactive
[postgres]$ createdb covid_chance
```

### General configuration

Configuration of all the steps is stored in a single JSON file.

Start by copying the sample configuration file
[config.sample.json](./config.sample.json) into any location. Example:

``` shell
$ cp config.sample.json ~/.config/covid-chance/config.json
```

Then configure the PostgreSQL database connection. Example:

```
{
  ...,
  "db": {
    "host": "/run/postgresql",
    "database": "covid_chance",
    "user": "postgres",
    "password": "postgres",
    "table_archives": "archives",
    "table_lines": "lines",
    "table_pages": "pages",
    "table_parsed": "parsed",
    "table_posted": "posted",
    "table_reviewed": "reviewed",
    "table_urls": "urls"
  },
  ...
}
```

### 1. Download feeds

The first step is to download RSS/Atom feeds and store the URLs of the pages
found in those feeds in the database.

#### Configuration

Configure this step by putting human-readable RSS/Atom feed names and URLs into
the object `feeds` of the configuration file.

Optionally, you can define a timeout in seconds for the download of one feed
using the `download_feeds.timeout` property.

Example:

```
{
  ...
  "feeds": [
    {
      "name": "Example Feed",
      "url": "https://www.example.com/rss.xml"
    },
    {
      "name": "My Second Feed",
      "url": "https://www.example.com/foo/atom"
    }
  ],
  "download_feeds": {
    "timeout": 30
  },
  ...
}
```

#### Running the step

``` shell
$ make download-feeds
```

This will download the latest version of the feeds defined in the configuration
and save the URLs of the pages in the database.

Running this command repeatedly will always download the latest version of the
feeds. That means this step is not idempotent.

- The default configuration file path is
  `~/.config/covid-chance/config.json`. You can specify a different location
  using the variable `config_path`.
- The default cache directory is `~/.cache/covid-chance/`. You can specify a
  different location using the variable `cache_dir`.

Example:

``` shell
$ make download-feeds config_path=~/my-other-config.json cache_path=~/my-cache
```

Example output:

```
Downloading feed https://www.example.com/rss.xml
done Example Feed                             1 urls inserted
```

This means 1 new URL has been found in the feed and was stored in the
database. All the other URLs in the feed were already in the database, so they
were not inserted again.

### 2. Download pages

The second step is to download the content of the pages from the URLs stored in
the database.

#### Configuration

This step doesn't have any required configuration.

Optionally, you can put a date in the format `YYYY-MM-DD` in the
`download_pages.since` property. URLs that were inserted in the database before
this date will not be downloaded. This is useful to limit the amount of pages to
download and lower the load on the server(s) you're downloading from.

Optionally, you can define a minimum and maximum time in seconds that the
program will wait before each page download using the `download_pages.wait_min`
and `download_pages.wait_max` properties. This is useful to lower the load on
the server(s) you're downloading from.

Optionally, you can define a timeout in seconds for the download of one page
using the `download_pages.timeout` property.

```
{
  ...
  "download_pages": {
    "since": "2020-03-01",
    "wait_min": 0,
    "wait_max": 0,
    "timeout": 10
  },
  ...
}
```

#### Running the step

``` shell
$ make download-pages
```

This will download the HTML content of all the pages from all the URLs stored in
the database, convert the HTML to plain text and store it to database.

Running this step repeatedly will not download the pages whose plain text
content is already stored in the database again. That means this step is
idempotent.

You can use the variables `config_path` and `cache_dir` to control the location
of the configuration file and the cache directory. See the documentation of the
*Download feeds* step.

Example output:

```
Selecting pages to download since 2020-03-01 00:00:00
Pages to download: 82
1/82 Downloading https://www.npr.org/sections/coronavirus-live-updates/2020/05/11/853886052/twitter-to-label-potentially-harmful-coronavirus-tweets
```

### 3. Search the text content

The third step is to search the text content of the pages for specified keywords
and store the text paragraphs that match in the database.

#### Configuration

Put the keywords to search the downloaded web pages for in the
`match_pages.keyword_lists` array. Each item of the array must be another array
of keywords. A paragraph is considered matching when it contains at least one
keyword from each of the arrays. The matching is case-insensitive. Example:

```
{
  ...
  "match_lines": {
    "keyword_lists": [
      ["coronavirus", "covid"],
      ["chance", "opportunity"]
    ]
  },
  ...
}
```

This configuration matches a paragraph that contains either "coronavirus" or
"covid" and a the same time either "chance" or "opportunity". The following
paragraphs will all match:

- "Coronavirus is a great opportunity."
- "Covid-19 is a great opportunity."
- "Coronavirus is a great chance."

But these paragraph will not:

- "Covid-19 can teach us something."
- "The lockdown is a great opportunity."

#### Running the step

``` shell
$ make match-lines
```

This will search the plain text content of all the pages stored in the database
and save those paragraphs that match the configured keywords.

Running this step repeatedly will not search the pages whose matching paragraphs
are already stored in the database again. That means this step is idempotent.

You can use the variables `config_path` and `cache_dir` to control the location
of the configuration file and the cache directory. See the documentation of the
*Download feeds* step.

Example output:

```
69876 Matched https://www.npr.org/sections/coronavirus-live-updates/2020/05/11/853886052/twitter-to-label-potentially-harmful-coronavirus-tweets
```

### 4. Create tweets

The fourth step is to create tweets from the stored paragraphs by applying a
regular expression to each paragraph. Paragraphs that don't match the regular
expression will be ignored.

#### Configuration

Put the regular expression in the property `parse_lines.pattern`. The regular
expression must define a group named `parsed` -- this will be the text of the
tweet.

```
{
  ...
  "parse_lines": {
    "pattern": "(?P<parsed>.+)"
  },
  ...
}
```

#### Running the step

``` shell
$ make parse-lines
```

This will apply the configured regular expression to all the paragraphs stored
in the database and store the content of the group named `parsed` for each of
them.

Running this step repeatedly will not process already processed paragraphs. That
means this step is idempotent.

You can use the variables `config_path` and `cache_dir` to control the location
of the configuration file and the cache directory. See the documentation of the
*Download feeds* step.

Example output:

```
26565 Parsed https://www.npr.org/sections/coronavirus-live-updates/2020/05/11/853886052/twitter-to-label-potentially-harmful-coronavirus-tweets
```

### 5. Review tweets

The fifth step is to manually review the created tweets using an interactive
command-line interface.

#### Configuration

This step doesn't have any required configuration.

Optionally, you can define the maximum tweet length in characters (doesn't
include shortened URL) using the property `review_tweets.max_tweet_length`, but
the default should be accurate. The interactive script will show a warning when
the text of the tweet exceeds the limit.

```
{
  ...
  "review_tweets": {
    "max_tweet_length": 247
  }
  ...
}
```

#### Running the step

``` shell
$ make review-tweets
```

This will start an interactive command-line interface that will ask you for each
of the created tweets stored in the database whether you like it or not and
offer an option to edit its text. The results will be again stored in the
database.

Running this step repeatedly will not ask you to review already reviewed
tweets. That means this step is idempotent.

If you would like to review again all tweets, run:

``` shell
$ make review-tweets-all
```

If you would like to review again all approved tweets, run:

``` shell
$ make review-tweets-approved
```

You can use the variables `config_path` and `cache_dir` to control the location
of the configuration file and the cache directory. See the documentation of the
*Download feeds* step.

### 6. Post tweet

The last step is to post an approved tweet to Twitter.

#### Configuration

Put you Twitter credentials in the file `~/.config/secrets.json`. Use
[secrets.sample.json](./secrets.sample.json) as a template.

Define the name of your Twitter profile in the property
`post_tweet.profile_name` and the description in the property
`post_tweet.profile_description_template`. This step will update your Twitter
profile with these values every time it's run. The description is a Python
template string that can contain the variables `${n_posted}` and `${n_total}`
which will be filled in with the number of tweets already posted and with the
total number of approved tweets respectively.

Optionally, you can defined a template string for each tweet in the
`post_tweet.tweet_template` property. This is a Python template string that can
contain the variables `${text}` and `${url}` which will be filled with the text
of the tweet and the URL of the page from which the text was parsed. This is
useful to include static hashtags in the tweet text.

```
{
  ...
  "post_tweet": {
    "profile_name": "Covid-19 is...",
    "profile_description_template": "${n_posted}/${n_total}",
    "tweet_template": "${text} ${url}"
  },
  ...
}
```

#### Running the step

``` shell
$ make post-tweet
```

This will post one random approved tweet to Twitter and set your Twitter profile
name and description according to the configuration.

Running this step repeatedly will not post already posted Tweets again. That
means this step is idempotent.

Alternatively, you can run this step in an interactive mode, which will ask you
using a command line for a confirmation before posting the tweet.

``` shell
$ make post-tweet-interactive
```

- The default secrets file path is `~/.config/covid-chance/secrets.json`. You
  can specify a different location using the variable `secrets_path`.
- You can use the variables `config_path` and `cache_dir` to control the
  location of the configuration file and the cache directory. See the
  documentation of the *Download feeds* step.

### Putting it all together

You can run several steps at the same time (in sequence) and in a cron job. Example:

```
*/30 * * * *  cd ~/covid-chance && make download-feeds download-pages match-lines parse-lines
0 * * * *  cd ~/covid-chance && make post-tweet
```

Such a crontab will download the feeds and pages and process them every half an
hour and post one tweet every hour. Now you just need to make sure to run `make
review-tweets` from time to time and approve some tweets.

### Other features

There are some more features such as downloading feeds from the Internet
Archive. For the list of all available commands, run:

``` shell
$ make help
```

## Development

### Installation

``` shell
$ make setup-dev
```

### Testing and linting

``` shell
$ make test
$ make lint
```

### Help

``` shell
$ make help
```

## Contributing

__Feel free to remix this project__ under the terms of the GNU General Public
License version 3 or later. See [COPYING](./COPYING) and [NOTICE](./NOTICE).
