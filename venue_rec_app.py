# tested with python2.7
from spyre import server
import venue_rec
import pandas as pd
pd.set_option('display.max_colwidth', -1)
venue_rec.offline_mode = False

vr = venue_rec.VenueRec()
init_artists = vr.get_artists_with_similar()

from bokeh.resources import INLINE
from bokeh.resources import CDN
from bokeh.embed import components
from bokeh.sampledata import us_states
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, GlyphRenderer 
from bokeh.models.glyphs import Circle
from collections import OrderedDict

init_artists_options = [{'label': "Select an Artist", 'value': " "}]
for i, row in init_artists.iterrows():
    init_artists_options.append({'label': row['name'], 'value': row['id']})

us_states = us_states.data.copy()
del us_states["HI"]
del us_states["AK"]


class VenueRecApp(server.App):
    title = "Venue Recommendations"

    inputs = [{
        "input_type": 'text',
        "label": 'songkick artist id',
        "value": 981579,
        "variable_name": 'artist_id',
        "action_id": "search",
        "linked_variable_name": "artist_by_name",
        "linked_variable_type": "dropdown",
        "linked_value": " "
    }, {
        "input_type": 'dropdown',
        "label": 'songkick name',
        "options": init_artists_options,
        "value": 981579,
        "variable_name": 'artist_by_name',
        "action_id": "search",
        "linked_variable_name": "artist_id",
        "linked_variable_type": "text",
    }, {
        "input_type": "slider",
        "label": "number of artists to compare against",
        "value": 20,
        "max": 50,
        "variable_name": "num_artists"
    }, {
        "input_type": "slider",
        "label": "number of venues to return",
        "value": 20,
        "max": 50,
        "variable_name": "num_venues"
    }, {
        "input_type": "checkboxgroup",
        "label": "exclude venues that this artist has played at recently",
        "options": [{"value": "x", "checked": True}],
        "variable_name": "exclude_recent"
    }]

    controls = [{
        "control_type": "button",
        "label": "Get Best Venues",
        "control_id": "search"
    }]

    outputs = [{
        "output_type": "html",
        "output_id": "html_map",
        "control_id": "search",
    }, {
        "output_type": "table",
        "output_id": "best_venues",
        "control_id": "search",
        "sortable": True
    }, {
        "output_type": "html",
        "output_id": "attribution"
    }]

    def __init__(self):
        self.vr = venue_rec.VenueRec()
        self.data_cache = None
        self.params_cache = None

    def getData(self, params):
        params.pop("output_id", None)  # caching layer
        if self.params_cache != params:  # caching layer
            artist_id = int(params['artist_id'])
            num_artists = int(params['num_artists'])
            num_venues = int(params['num_venues'])
            exclude_recent = params['exclude_recent']
            top_venues = self.vr.get_top_venues(artist_id, num_artists)
            if len(exclude_recent) > 0:
                recent_venues = self.vr.crawl_songkick([artist_id])
                top_venues_df = self.vr.get_venues(top_venues, num_venues, True, artist_id)
            else:
                top_venues_df = self.vr.get_venues(top_venues, num_venues)
            top_venues_df = top_venues_df.loc[
                (top_venues_df['lng'] > -130) & (top_venues_df['lat'] < 55), :
            ]
            self.data_cache = top_venues_df     # caching layer
            self.params_cache = params          # caching layer
        return self.data_cache

    def getTable(self, params):
        df = self.getData(params).copy()
        df['name'] = "<a href='https://www.songkick.com/venues/" + df.loc[:, 'id'].astype(str) + "' target='_blank'>" + df.loc[:, 'name'] + "</a>"
        return df.drop(['id', 'lat', 'lng'], axis=1)

    def html_map(self, params):
        artist_id = int(params['artist_id'])
        ### implements a simple caching mechanism to avoid multiple calls to the yahoo finance api ###
        params.pop("output_id", None)
        import time
        while self.params_cache != params:
            time.sleep(0.1)
        ###############################################################################################
        df = self.getData(params)
        artist_name = self.vr.get_artist_name(artist_id)
        state_xs = [us_states[code]["lons"] for code in us_states]
        state_ys = [us_states[code]["lats"] for code in us_states]
        title = "Recommended Venues for %s" % artist_name
        p = figure(
            title=title, toolbar_location="right", plot_width=600, plot_height=400,
            tools='hover, box_zoom, reset', title_text_font_size='12pt'
        )
        p.patches(state_xs, state_ys, fill_alpha=0.0, line_width=2)
        p.axis.visible = None
        p.xgrid.grid_line_color = None
        p.ygrid.grid_line_color = None

        source = ColumnDataSource(
            data=dict(
                lat=df['lat'],
                lon=df['lng'],
                name=df['name'],
                city=df['city'],
                capacity=df['capacity'],
                score=df['score']
            )
        )

        hover = p.select(dict(type=HoverTool))
        hover.tooltips = OrderedDict([
            ("venue", "@name"),
            ("match score", "@score"),
            ("city", "@city"),
            ("capacity", "@capacity")
        ])
        hover.point_policy = 'snap_to_data'
        cty_glyph = Circle(x="lon", y="lat", size=10)
        cty = GlyphRenderer(data_source=source, glyph=cty_glyph)
        hover.renderers = [cty]
        p.renderers.extend([cty])

        script, div = components(p, CDN)
        html = "%s\n%s" % (script, div)
        return html

    def getCustomJS(self):
        return INLINE.js_raw[0]

    def getCustomCSS(self):
        return INLINE.css_raw[0]

    def attribution(self, params):
        html = "It can take up to a minute to load the data for an artist for the first time. Be patient!<br>"
        html += "<img src='http://ext-assets.sennheiser.com/sennhubus/public/artists/images/concerts-by-songkick.jpg'>"
        return html


if __name__ == '__main__':
    app = VenueRecApp()
    app.launch(host='0.0.0.0', port=8081, prefix='/spyre/venue_rec')
