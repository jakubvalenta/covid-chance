import logging
import random
from string import Template
from typing import Dict

import twitter

from covid_chance.model import (
    PostedTweet, Tweet, TweetReviewStatus, count, create_session,
)
from covid_chance.utils.dict_utils import deep_get

logger = logging.getLogger(__name__)


def update_profile(
    name: str, description: str, secrets: Dict[str, str], dry_run: bool = True
):
    if dry_run:
        logger.warning('This is just a dry run, not calling Twitter API')
        return False
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token_key'],
        access_token_secret=secrets['access_token_secret'],
    )
    user = api.UpdateProfile(name=name, description=description)
    logger.warning('Updated profile of user %s', user.name)


def post_tweet(text: str, secrets: Dict[str, str], dry_run: bool = True):
    if dry_run:
        logger.warning('This is just a dry run, not calling Twitter API')
        return False
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token_key'],
        access_token_secret=secrets['access_token_secret'],
    )
    status = api.PostUpdate(status=text)
    logger.warning(
        'Posted tweet "%s" as user %s', status.text, status.user.name
    )
    return True


def main(config: dict, secrets: dict, interactive: bool, dry_run: bool):
    session = create_session(config['db']['url'])
    approved_tweets = session.query(Tweet).filter(
        Tweet.status == TweetReviewStatus.approved
    )
    posted_tweets = session.query(PostedTweet).all()
    posted_tweets_parsed = [t.parsed for t in posted_tweets]
    pending_tweets = [
        t for t in approved_tweets if t.parsed not in posted_tweets_parsed
    ]
    total_approved_tweets = count(approved_tweets)
    total_posted_tweets = len(posted_tweets)
    total_pending_tweets = len(pending_tweets)

    logger.info('Number of approved tweets: %d', total_approved_tweets)
    logger.info('Number of posted tweets:   %d', total_posted_tweets)
    logger.info('Number of tweets to post:  %d', total_pending_tweets)

    if not total_pending_tweets:
        logger.warning('Nothing to do, all tweets have already been posted')
        return

    i = random.randint(0, total_pending_tweets - 1)
    tweet = pending_tweets[i]
    template_str = deep_get(
        config, ['post_tweet', 'tweet_template'], default='${text} ${url}'
    )
    text = Template(template_str).substitute(text=tweet.text, url=tweet.url)

    logger.warning(
        '%d/%d/%d posting tweet "%s"',
        i,
        total_pending_tweets,
        total_approved_tweets,
        text,
    )
    if interactive:
        inp = input('Are you sure you want to post this tweet? [y/N] ')
        if inp != 'y':
            print('Bailing out!')
            return
    post_tweet(text, secrets, dry_run)

    posted_tweet = PostedTweet.from_tweet(tweet, text)
    session.add(posted_tweet)
    session.commit()

    name = config['post_tweet']['profile_name']
    description = Template(
        config['post_tweet']['profile_description_template']
    ).substitute(
        n_posted=total_posted_tweets + 1, n_approved=total_approved_tweets
    )
    logger.warning(
        'Updating profile, name: "%s", description: "%s"',
        name,
        description,
    )
    update_profile(name, description, secrets, dry_run)
    session.close()
