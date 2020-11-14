import logging
import readline
from textwrap import fill
from typing import Optional

import colored
from sqlalchemy import inspect

from covid_chance.model import (
    PageLine, ParsedPageLine, Tweet, TweetReviewStatus, count, create_session,
)

logger = logging.getLogger(__name__)


def rlinput(prompt, prefill: str = '') -> Optional[str]:
    """See https://stackoverflow.com/a/36607077"""
    readline.set_startup_hook(lambda: readline.insert_text(prefill))
    try:
        return input(prompt)
    finally:
        readline.set_startup_hook()


def highlight_substr(s: str, substr: str, fg_color: int = 2) -> str:
    return s.replace(substr, colored.stylize(substr, colored.fg(fg_color)))


def print_tweet(
    tweet: Tweet,
    i: Optional[int] = None,
    total: Optional[int] = None,
    highlight: bool = False,
    counter_width: int = 10,
    line_width: int = 80,
):
    print('-' * line_width)
    print(
        '/'.join(str(num) for num in (i, total) if num is not None).ljust(
            counter_width
        ),
        end='',
    )
    print((tweet.status or '').upper(), end='')
    if tweet.invalid:
        print(
            '  ',
            colored.stylize(
                f'too long ({len(tweet.text)}/{tweet.MAX_TWEET_LENGTH})',
                colored.fg('red'),
            ),
            end='',
        )
    print()
    print()
    print(tweet.url)
    print()
    if highlight:
        s = highlight_substr(tweet.line, tweet.parsed)
    else:
        s = tweet.line
    print(fill(s, line_width))
    print()
    if tweet.edited:
        print(colored.stylize(tweet.edited, colored.fg('red')))
        print()


def main(config, review_all: bool, incl_approved: bool):
    session = create_session(config['db']['url'])

    parsed_page_lines = session.query(ParsedPageLine).filter(
        ParsedPageLine.parsed != ''
    )
    reviewed_tweets = session.query(Tweet).filter(
        Tweet.status != TweetReviewStatus.none
    )
    approved_tweets = [
        t for t in reviewed_tweets if t.status == TweetReviewStatus.approved
    ]
    rejected_tweets = [
        t for t in reviewed_tweets if t.status == TweetReviewStatus.rejected
    ]
    if review_all:
        pending_parsed_page_lines = parsed_page_lines
    else:
        reviewed_tweets_parsed = [
            tweet.parsed
            for tweet in session.query(Tweet).filter(
                Tweet.status != TweetReviewStatus.none
            )
        ]
        pending_parsed_page_lines = parsed_page_lines.filter(
            ParsedPageLine.parsed.notin_(reviewed_tweets_parsed)
        )
    pending_tweets = [
        Tweet.from_parsed_page_line(parsed_page_line)
        for parsed_page_line in pending_parsed_page_lines
    ]
    if not review_all:
        if incl_approved:
            pending_tweets += approved_tweets
        else:
            invalid_approved_tweets = [t for t in approved_tweets if t.invalid]
            pending_tweets += invalid_approved_tweets
    total_pending_tweets = len(pending_tweets)

    logger.info(
        'Number of matching lines:   %d', session.query(PageLine).count()
    )
    logger.info('Number of parsed tweets:    %d', count(parsed_page_lines))
    logger.info('Number of approved tweets:  %d', len(approved_tweets))
    logger.info('Number of rejected tweets:  %d', len(rejected_tweets))
    logger.info('Number of tweets to review: %d', total_pending_tweets)

    i = 0
    while i < len(pending_tweets):
        tweet = pending_tweets[i]
        print_tweet(
            tweet,
            i=i + 1,
            total=total_pending_tweets,
            highlight=True,
        )
        inp = None
        while inp is None or (inp not in ('y', 'n', 'e', 'q', 's', 'p', '')):
            inp = rlinput(
                'Do you like this tweet? '
                '"y" or Enter = yes, '
                '"n" = no, '
                '"e" = edit, '
                '"s" = skip (ask next time again), '
                '"p" = show previous tweet, '
                '"q" = quit \n'
                '> '
            )
        if inp == 'q':
            break
        if inp == 's':
            i = i + 1
            continue
        if inp == 'p':
            i = max(i - 1, 0)
            continue
        if inp in ('y' or ''):
            tweet.status = TweetReviewStatus.approved
        elif inp == 'n':
            tweet.status = TweetReviewStatus.rejected
        elif inp == 'e':
            edited_text = None
            while edited_text is None:
                edited_text = rlinput(
                    'Enter new text or delete it to reject the tweet.\n> ',
                    tweet.edited or tweet.parsed,
                )
            tweet.edited = edited_text
            if edited_text == '':
                tweet.status = TweetReviewStatus.rejected
            else:
                tweet.status = TweetReviewStatus.approved
        else:
            raise NotImplementedError('Invalid input')
        if inspect(tweet).transient:
            session.add(tweet)
        session.commit()
        i = i + 1
    session.close()
