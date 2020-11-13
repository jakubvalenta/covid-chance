import logging
import string
from pathlib import Path
from typing import IO, List, Optional, Sequence

import dateutil.tz
from jinja2 import BaseLoader, Environment, FileSystemLoader, PackageLoader

from covid_chance.model import ExportedTweet, create_session

logger = logging.getLogger(__name__)


def render_template(
    f: IO,
    tweets: List[ExportedTweet],
    context: dict,
    package: Optional[Sequence[str]] = None,
    path: Optional[str] = None,
):
    if package:
        loader: BaseLoader = PackageLoader(*package[:-1])
        template_name = package[-1]
    elif path:
        p = Path(path)
        loader = FileSystemLoader(p.parent)
        template_name = p.name
    else:
        raise Exception('Invalid arguments')
    environment = Environment(loader=loader)
    template = environment.get_template(template_name)
    stream = template.stream(tweets=tweets, **context)
    f.writelines(stream)


def main(config: dict, output_dir: Path):
    session = create_session(config['db']['url'])

    default_tz = dateutil.tz.gettz(config['print_export']['default_tz'])
    if not default_tz:
        raise Exception('Invalid time zone')
    context = {'default_tz': default_tz, **config['print_export']['context']}
    exported_tweets = session.query(ExportedTweet).all()
    tweets_per_page = config['print_export']['tweets_per_page']

    output_filename_template = string.Template(
        config['print_export']['output_filename_template']
    )
    for i in range(len(exported_tweets) // tweets_per_page + 1):
        start = i * tweets_per_page
        end = start + tweets_per_page
        tweets = exported_tweets[start:end]
        if not tweets:
            return
        output_filename = output_filename_template.substitute(
            i=f'{i:03}', **tweets[0].to_dict()
        )
        output_path = output_dir / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('w') as f:
            render_template(
                f,
                tweets=tweets,
                context=context,
                package=config['print_export'].get('template_package'),
                path=config['print_export'].get('template_path'),
            )
