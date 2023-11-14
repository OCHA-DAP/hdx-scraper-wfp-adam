#!/usr/bin/python
"""
WFP ADAM:
--------

Reads WFP ADAM data and creates datasets.

"""
import logging
import re

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import get_filename_extension_from_url
from slugify import slugify

logger = logging.getLogger(__name__)


def lazy_fstr(template, properties):
    return eval(f'f"""{template}"""')


class ADAM:
    regex = re.compile(r".*/(.*)/(.*)")

    def __init__(self, configuration, retriever, today):
        self.configuration = configuration
        self.retriever = retriever
        self.today = today.date().isoformat()
        self.last_build_date = None
        self.latest_episodes = {}
        self.events = []

    def parse_feed(self, previous_build_date):
        url = self.configuration["url"]
        start_date = previous_build_date.date().isoformat()
        url = f"{url}feed?start_date={start_date}&end_date={self.today}"
        for event in self.retriever.download_json(url):
            countryiso = event["eventISO3"]
            if not countryiso:
                logger.error(f"Blank eventISO3!")
                continue
            countryinfo = Country.get_country_info_from_iso3(countryiso)
            income_level = countryinfo['#indicator+incomelevel']
            if income_level.lower() == "high":
                logger.info(f"ignoring high income country {countryiso}!")
                continue
            published = parse_date(event["pubDate"])
            if published <= previous_build_date:
                continue
            m = self.regex.match(event["eventDetails"])
            event_type = m.group(1)
            eventtype_info = self.configuration["event_types"].get(event_type)
            if not eventtype_info:
                continue
            event["event_type"] = event_type
            guid = event["guid"]
            parts = guid.split("_")
            event_id_index = eventtype_info["event_id_index"]
            event_id = parts[event_id_index]
            prefix_index = eventtype_info["prefix_index"]
            if prefix_index is not None:
                prefix = parts[prefix_index]
                if prefix not in eventtype_info["allowed_prefixes"]:
                    continue
                event_id = f"{prefix}_{event_id}"
            episode_id_index = eventtype_info["episode_id_index"]
            if episode_id_index is None:
                episode_id = None
            else:
                episode_id = parts[episode_id_index]
            prev_event = self.latest_episodes.get(event_id)
            if prev_event is None or (
                episode_id and episode_id > prev_event["episode_id"]
            ):
                event["event_id"] = event_id
                event["episode_id"] = episode_id
                self.latest_episodes[event_id] = event

    def parse_eventtype_feed(self, event):
        json = self.retriever.download_json(event["eventDetails"])
        features = json.get("features")
        if features:
            episode_ids = []
            for feature in features:
                episode_ids.append(feature["properties"]["episode_id"])
            if not episode_ids:
                return
            properties = features[0]["properties"]
        else:
            episode_ids = None
            properties = json["properties"]
        countryiso = properties["iso3"]
        if not countryiso:
            properties["iso3"] = event["eventISO3"]
        event["properties"] = properties
        event_id = event["event_id"]
        event_type = event["event_type"]
        eventtype_info = self.configuration["event_types"].get(event_type)
        name = eventtype_info["name"]
        event["name"] = lazy_fstr(name, properties)
        title = eventtype_info["title"]
        event["title"] = lazy_fstr(title, properties)
        description = eventtype_info["description"]
        event["description"] = lazy_fstr(description, properties)
        self.events.append(
            {"event_id": event_id, "title": title, "episode_ids": episode_ids}
        )

    def parse_eventtypes_feeds(self):
        for event in self.latest_episodes.values():
            self.parse_eventtype_feed(event)

    def get_events(self):
        return self.events

    def generate_dataset(
        self,
        event,
    ):
        """ """
        event_id = event["event_id"]
        episode = self.latest_episodes[event_id]
        properties = episode["properties"]
        name = episode["name"]
        title = episode["title"]
        countryiso = properties["iso3"]
        countryname = Country.get_country_name_from_iso3(countryiso)
        slugified_name = slugify(f"{countryname}{name[3:]}")
        title = f"{countryname}{title[3:]}"
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
                "notes": episode["description"],
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("1ca198b6-e490-4cd0-9c1a-5b91bad9879a")
        dataset.set_expected_update_frequency("Never")
        dataset.set_subnational(True)
        dataset.add_country_location(countryiso)
        event_type = episode["event_type"]
        tags = [event_type]
        from_date = properties.get("from_date")
        to_date = properties.get("to_date")
        published_at = properties["published_at"]
        if from_date and to_date:
            dataset.set_reference_period(from_date, to_date)
        else:
            dataset.set_reference_period(published_at)

        def add_resource(url, description):
            filename, extension = get_filename_extension_from_url(url)
            resource = Resource(
                {
                    "name": f"{filename}{extension}",
                    "url": url,
                    "description": description,
                }
            )
            if description == "Shape File":
                resource.set_file_type("shp")
            else:
                resource.set_file_type(extension[1:])
            resource.set_date_data_updated(published_at)
            dataset.add_update_resource(resource)

        url_dict = properties["url"]
        shape_url = url_dict.get("shapefile")
        url = url_dict.get("population_csv", url_dict.get("population"))
        if not shape_url and not url:
            logger.error(f"{title} has no data files for dataset!")
            return None, None
        if shape_url:
            add_resource(shape_url, "Shape File")
            tags.append("geodata")

        if url:
            add_resource(url, "Population Estimation")
            tags.append("affected population")

        def view_image(url_type):
            viewer_url = "https://mcarans.github.io/view-images/#"
            map_url = url_dict.get(url_type)
            if not map_url:
                return None, None
            return f"{viewer_url}{map_url}", map_url

        dataset.add_tags(tags)
        showcases = []

        def add_showcase(title, description, url):
            showcase = Showcase(
                {
                    "name": f"{slugified_name}-{slugify(title)}-showcase",
                    "title": title,
                    "notes": description,
                    "url": url,
                    "image_url": url,
                }
            )
            showcase.add_tags(tags)
            showcases.append(showcase)

        preview_url, url = view_image("map")
        if url:
            dataset["customviz"] = [{"url": preview_url}]
            add_showcase("Map", "Map", url)

        url = url_dict.get("shakemap")
        if url:
            if dataset.get("customviz") is None:
                dataset["customviz"] = [{"url": preview_url}]
            add_showcase("Shake Map", "Shake Map", url)
        preview_url, url = view_image("wind")
        if url:
            if dataset.get("customviz") is None:
                dataset["customviz"] = [{"url": preview_url}]
            add_showcase("Wind Map", "Wind Map", url)
        url = url_dict.get("rainfall")
        if url:
            if dataset.get("customviz") is None:
                dataset["customviz"] = [{"url": preview_url}]
            add_showcase("Rainfall Map", "Rainfall Map", url)
        dataset.preview_off()
        return dataset, showcases
