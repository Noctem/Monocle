from datetime import datetime
import json

from flask import Flask, render_template

from web import get_args  # pretty handy function, actually
import config
import db
import utils


with open('locales/pokemon.en.json') as f:
    pokemon_names = json.load(f)


app = Flask(__name__, template_folder='templates')


def calculate_stats(forts):
    count = {t.value: 0 for t in db.Team}
    strongest = {t.value: None for t in db.Team}
    percentages = {}
    for fort in forts:
        team = fort['team']
        count[team] = count[team] + 1
        existing = strongest[team]
        should_replace = (
            existing is not None and
            fort['prestige'] > existing[0] or
            existing is None
        )
        if should_replace:
            strongest[team] = (
                fort['prestige'],
                fort['pokemon_id'],
                pokemon_names[str(fort['pokemon_id'])],
            )
    for team in db.Team:
        percentages[team.value] = count.get(team.value) / len(forts)
    return {
        'order': sorted(count, key=count.__getitem__, reverse=True),
        'count': count,
        'total_count': sum(count),
        'strongest': strongest,
        'percentages': percentages,
        'generated_at': datetime.now()
    }


@app.route('/')
def index():
    session = db.Session()
    forts = db.get_forts(session)
    stats = calculate_stats(forts)
    session.close()
    team_names = {k.value: k.name.title() for k, v in db.Team}
    styles = {1: 'danger', 2: 'primary', 3: 'warning'}
    return render_template(
        'gyms.html',
        area_name=config.AREA_NAME,
        area_size=utils.get_scan_area(),
        minutes_ago=int((datetime.now() - stats['generated_at']).seconds / 60),
        team_names=team_names,
        styles=styles,
        **stats
    )


if __name__ == '__main__':
    args = get_args()
    app.run(debug=True, host=args.host, port=args.port)
