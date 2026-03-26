#!/usr/bin/python
"""
WFP ADAM:
--------

Reads WFP ADAM data and creates datasets.

"""

import logging
import re
from datetime import datetime
from os.path import basename, splitext

from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.scraper.wfp.adam.utilities import get_latitude_longitude
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%dT00:00:00Z"


def lazy_fstr(template, event):
    return eval(f'f"""{template}"""')


class Pipeline:
    regex = re.compile(r".*/(.*)/(.*)")

    def __init__(self, configuration, retriever, today, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.today = today.strftime(DATE_FORMAT)
        self.folder = folder
        self.last_build_date = None
        self.events = []

    #   https://api.adam.geospatial.wfp.org/api/collections/adam.adam_eq_events/items?datetime-column=published_at&limit=10&datetime=2025-09-01T00%3A00%3A00Z%2F2026-03-24T01%3A53%3A00Z&filter=published%3Dtrue&f=json
    #   https://api.adam.geospatial.wfp.org/api/collections/adam.adam_ts_events/items?datetime-column=published_at&limit=10&datetime=2025-09-01T00%3A00%3A00Z%2F2026-03-24T01%3A53%3A00Z&filter=published%3Dtrue&f=json
    #
    def get_all_events(self, previous_run_date: datetime, collection_id: str) -> list:
        base_url = self.configuration["url"]
        start_date = previous_run_date.strftime(DATE_FORMAT)
        base_url = f"{base_url}/{collection_id}/items?f=json&limit=10000&filter=published%3Dtrue&datetime-column=published_at&datetime={start_date}/{self.today}"
        events = self.retriever.download_json(base_url)
        return events

    def parse_feed(self, previous_run_date: datetime, eventtype_info: dict) -> dict:
        latest_episodes = {}
        collection_id = eventtype_info["collection_id"]
        for event in self.get_all_events(previous_run_date, collection_id):
            if not event["published"]:
                continue
            event_type = event.get("event_type")
            if event_type and event_type != eventtype_info["event_type"]:
                continue
            countryiso3 = event["iso3"]
            if not countryiso3:
                logger.error("Blank iso3!")
                continue
            countryinfo = Country.get_country_info_from_iso3(countryiso3)
            income_level = countryinfo["World Bank Income Level"] or ""
            if income_level.lower() == "high":
                logger.info(f"ignoring high income country {countryiso3}!")
                continue
            published_at = parse_date(event["published_at"])
            if published_at <= previous_run_date:
                continue
            key = event.get("event_id", event["uid"])
            episode_id = event.get("episode_id")
            prev_event = latest_episodes.get(key)
            if prev_event:
                if episode_id:
                    if episode_id > prev_event["episode_id"]:
                        event["episode_ids"] = prev_event["episode_ids"]
                        event["episode_ids"].add(episode_id)
                        latest_episodes[key] = event
                    else:
                        prev_event["episode_ids"].add(episode_id)
            else:
                event["episode_ids"] = {episode_id}
                latest_episodes[key] = event
        return latest_episodes

    def parse_eventtypes_feed(
        self, latest_episodes: dict, eventtype_info: dict
    ) -> list:
        name = eventtype_info["name"]
        title = eventtype_info["title"]
        description = eventtype_info["description"]
        events = []
        for event in latest_episodes.values():
            event["name"] = lazy_fstr(name, event)
            event["title"] = lazy_fstr(title, event)
            event["latitude"], event["longitude"] = get_latitude_longitude(
                event["geometry"]
            )
            event["description"] = lazy_fstr(description, event)
            key = event.get("event_id", event["uid"])
            events.append(
                {
                    "key": key,
                    "title": title,
                    "episode_ids": event["episode_ids"],
                    "tag": eventtype_info["tag"],
                }
            )
        return events

    def generate_dataset(
        self,
        latest_episodes: dict,
        event: dict,
    ):
        """ """
        episode = latest_episodes[event["key"]]
        name = episode["name"]
        title = episode["title"]
        countryiso = episode["iso3"]
        countryname = Country.get_country_name_from_iso3(countryiso)
        slugified_name = slugify(f"{countryname}{name[3:]}")
        title = f"{countryname}{title[3:]}"
        logger.info(f"Creating dataset: {title}")
        uid = episode["uid"].replace("_", "\_")
        description = f"**ADAM ID: {uid}**  {episode['description']}"
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
                "notes": description,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("1ca198b6-e490-4cd0-9c1a-5b91bad9879a")
        dataset.set_expected_update_frequency("Never")
        dataset.set_subnational(True)
        dataset.add_country_location(countryiso)
        tags = {event["tag"]}
        dataset.set_time_period(episode["published_at"])

        def add_resource(path, description):
            name = basename(path)
            filename, extension = splitext(name)
            resource = Resource(
                {
                    "name": name,
                    "description": description,
                }
            )
            if description == "Shape File":
                extension = "shp"
            else:
                extension = extension[1:]
            resource.set_format(extension)
            resource.set_file_to_upload(path)
            return resource

        def add_resource_with_url(url, description):
            try:
                path = self.retriever.download_file(url)
                return add_resource(path, description)
            except DownloadError as ex:
                logger.exception(ex)
                return None

        showcases = []

        def add_showcase(title, description, url, image_url=None):
            if image_url is None:
                image_url = url
            showcase = Showcase(
                {
                    "name": f"{slugified_name}-{slugify(title)}-showcase",
                    "title": title,
                    "notes": description,
                    "url": url,
                    "image_url": image_url,
                }
            )
            showcase.add_tags(sorted(tags))
            showcases.append(showcase)

        resources = []
        geojson_url = episode.get("detail_url")
        shape_url = episode.get("shapefile_url")
        url = episode.get("population_csv_url")
        if not geojson_url and not shape_url and not url:
            logger.error(f"{title} has no data files for dataset!")
            return None, None
        if geojson_url:
            resource = add_resource_with_url(geojson_url, "GeoJSON File")
            if resource:
                resources.append(resource)
            tags.add("geodata")
        if shape_url:
            resource = add_resource_with_url(shape_url, "Shape File")
            if resource:
                resources.append(resource)
            tags.add("geodata")
        if url:
            resource = add_resource_with_url(url, "Population Estimation")
            if resource:
                resources.append(resource)
            tags.add("affected population")
        if len(resources) == 0:
            return None, None
        dataset.add_update_resources(resources)
        dataset.preview_off()
        dataset.add_tags(sorted(tags))
        dashboard_url = episode.get("dashboard_url")
        if not dashboard_url:
            for resource in resources:
                if resource["description"] == "Shape File":
                    resource.enable_dataset_preview()
                    dataset.preview_resource()
            return dataset, showcases

        def view_image(map_url):
            viewer_url = "https://mcarans.github.io/view-images/#"
            return f"{viewer_url}{map_url}"

        preview_url = view_image(dashboard_url)
        dataset["customviz"] = [{"url": preview_url}]
        add_showcase("Map", "Map", dashboard_url)

        return dataset, showcases
