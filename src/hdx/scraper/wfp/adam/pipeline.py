#!/usr/bin/python
"""
WFP ADAM:
--------

Reads WFP ADAM data and creates datasets.

"""

import logging
import re
from datetime import datetime

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date
from hdx.utilities.url import get_filename_extension_from_url
from slugify import slugify

from hdx.scraper.wfp.adam.utilities import get_latitude_longitude

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%dT00:00:00"


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

    def get_all_events(
        self, previous_run_date: datetime, collection_url: str, event_type: str
    ) -> list:
        start_date = previous_run_date.strftime(DATE_FORMAT)
        if event_type == "FL":
            url = f"{collection_url}?f=json&limit=10000&filter=cleared%3D'yes'&effective%3E'{start_date}'&sortby=-effective"
        else:
            url = f"{collection_url}?f=json&limit=10000&filter=published%3Dtrue%20AND%20published_at%3ETIMESTAMP('{start_date}')&sortby=-published_at"
        events = self.retriever.download_json(url)
        return events

    def parse_feed(self, previous_run_date: datetime, eventtype_info: dict) -> dict:
        latest_episodes = {}
        collection_id = eventtype_info["collection_id"]
        event_type_code = eventtype_info["event_type_code"]
        base_url = self.configuration["url"]
        collection_url = f"{base_url}/{collection_id}/items"
        for event in self.get_all_events(
            previous_run_date, collection_url, event_type_code
        ):
            event_type = event.get("event_type")
            if event_type and event_type != event_type_code:
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
            published_at = parse_date(event.get("published_at") or event["effective"])
            if published_at <= previous_run_date:
                continue
            for keyname in ("event_id", "uid", "eventid"):
                key = event.get(keyname)
                if key:
                    break
            event["event_url"] = f"{collection_url}?{keyname}={key}"
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
        resource_name = eventtype_info["resource_name"]
        resource_description = eventtype_info["resource_description"]
        events = []
        for key, event in latest_episodes.items():
            event["name"] = lazy_fstr(name, event)
            event["title"] = lazy_fstr(title, event)
            event["latitude"], event["longitude"] = get_latitude_longitude(
                event["geometry"]
            )
            event["description"] = lazy_fstr(description, event)
            event["resource_name"] = lazy_fstr(resource_name, event).lower()
            event["resource_description"] = lazy_fstr(resource_description, event)
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
        slugified_name = slugify(f"{countryiso}{name[3:]}")
        title = f"{countryname}{title[3:]}"
        logger.info(f"Creating dataset: {title}")
        evid = (episode.get("uid") or episode.get("eventid", "")).replace("_", "\\_")
        description = (
            f"**ADAM ID: **[{evid}]({episode['event_url']})  {episode['description']}"
        )
        description_extra = self.configuration["description_extra"]
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
                "notes": f"{description}  \n  \n{description_extra}",
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("1ca198b6-e490-4cd0-9c1a-5b91bad9879a")
        dataset.set_expected_update_frequency("Never")
        dataset.set_subnational(True)
        dataset.add_country_location(countryiso)
        tags = {event["tag"]}
        dataset.set_time_period(episode.get("published_at") or episode["effective"])

        resource_name = episode["resource_name"]

        def add_resource(url, description, format=None):
            _, extension = get_filename_extension_from_url(url)
            filename = f"{resource_name}{extension}"
            try:
                path = self.retriever.download_file(url, filename=filename)
                resource = Resource(
                    {
                        "name": filename,
                        "description": description,
                    }
                )
                if not format:
                    format = extension[1:]
                resource.set_format(format)
                resource.set_file_to_upload(path)
                return resource
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
        data_pkg_url = episode.get("data_pkg")
        output_table_url = episode.get("output_table_url")
        if not geojson_url and not shape_url and not url and not data_pkg_url:
            logger.error(f"{title} has no data files for dataset!")
            return None, None
        resource_description = episode["resource_description"]
        if geojson_url:
            resource = add_resource(
                geojson_url, resource_description.replace("Data", "GeoJSON")
            )
            if resource:
                resources.append(resource)
            tags.add("geodata")
        if shape_url:
            resource = add_resource(
                shape_url, resource_description.replace("Data", "Shapefile"), "shp"
            )
            if resource:
                resources.append(resource)
            tags.add("geodata")
        if url:
            resource = add_resource(
                url, resource_description.replace("Data", "Population estimation")
            )
            if resource:
                resources.append(resource)
            tags.add("affected population")
        if data_pkg_url:
            resource = add_resource(
                data_pkg_url, resource_description.replace("Data", "GeoTIFF"), "geotiff"
            )
            if resource:
                resources.append(resource)
            tags.add("geodata")
        if output_table_url:
            resource = add_resource(
                output_table_url,
                resource_description.replace("Data", "Population estimation"),
            )
            if resource:
                resources.append(resource)
            tags.add("affected population")
        if len(resources) == 0:
            return None, None
        dataset.add_update_resources(resources)
        dataset.preview_off()
        dataset.add_tags(sorted(tags))
        dashboard_url = episode.get("dashboard_url")
        prod_url = episode.get("prod_url")
        if not dashboard_url:
            for resource in resources:
                if resource["description"] == "Shape File":
                    resource.enable_dataset_preview()
                    dataset.preview_resource()
            if prod_url:
                add_showcase(
                    "Flood Report",
                    "Flood Report",
                    prod_url,
                    image_url="https://multimedia.wfp.org/AssetLink/5c46vkj768qd0n6e12327eq14y857i8h.jpg",
                )
            return dataset, showcases

        def view_image(map_url):
            viewer_url = "https://mcarans.github.io/view-images/#"
            return f"{viewer_url}{map_url}"

        preview_url = view_image(dashboard_url)
        dataset["customviz"] = [{"url": preview_url}]
        add_showcase("Map", "Map", dashboard_url)

        return dataset, showcases
