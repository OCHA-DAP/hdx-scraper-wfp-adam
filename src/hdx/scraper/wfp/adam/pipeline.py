#!/usr/bin/python
"""
WFP ADAM:
--------

Reads WFP ADAM data and creates datasets.

"""

import logging
import re
from os import rename
from os.path import basename, splitext
from zipfile import ZipFile

from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


def lazy_fstr(template, properties):
    return eval(f'f"""{template}"""')


class Pipeline:
    regex = re.compile(r".*/(.*)/(.*)")

    def __init__(self, configuration, retriever, today, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.today = today.date().isoformat()
        self.folder = folder
        self.last_build_date = None
        self.latest_episodes = {}
        self.events = []

    def get_all_events(self, previous_build_date):
        base_url = self.configuration["url"]
        start_date = previous_build_date.date().isoformat()
        base_url = (
            f"{base_url}?startDate={start_date}&endDate={self.today}&items_per_page=500"
        )
        json = self.retriever.download_json(base_url)
        events = json["events"]
        pages = json["pages"]
        for page in range(2, pages + 1):
            url = f"{base_url}&page={page}"
            json = self.retriever.download_json(url)
            events.extend(json["events"])
        return events

    def parse_feed(self, previous_build_date):
        for event in self.get_all_events(previous_build_date):
            countryiso3 = event["iso3"]
            if not countryiso3:
                logger.error("Blank iso3!")
                continue
            countryinfo = Country.get_country_info_from_iso3(countryiso3)
            income_level = countryinfo["World Bank Income Level"] or ""
            if income_level.lower() == "high":
                logger.info(f"ignoring high income country {countryiso3}!")
                continue
            published = parse_date(event["pubDate"])
            if published <= previous_build_date:
                continue
            event_type = event["eventType"]
            eventtype_info = self.configuration["event_types"].get(event_type)
            if not eventtype_info:
                continue
            guid = event["guid"]
            parts = guid.split("_")
            event_id_index = eventtype_info["event_id_index"]
            event_id = parts[event_id_index]
            prefix_index = eventtype_info["prefix_index"]
            if prefix_index is not None:
                prefix = parts[prefix_index]
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
        json = self.retriever.download_json(event["details"])
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
            properties["iso3"] = event["iso3"]
        event_id = event["event_id"]
        if event_id not in properties:
            properties["event_id"] = event_id
        event["properties"] = properties
        event_type = event["eventType"]
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
        guid = episode["guid"].replace("_", "\_")
        description = f"**ADAM ID: {guid}**  {episode['description']}"
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
        event_type = episode["eventType"].lower()
        tags = [event_type]
        from_date = properties.get("from_date")
        to_date = properties.get("to_date")
        published_at = properties.get("published_at", properties.get("effective_date"))
        if from_date and to_date:
            dataset.set_time_period(from_date, to_date)
        else:
            dataset.set_time_period(published_at)

        def add_resource(path, description, preview=False):
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
            dataset.add_update_resource(resource)
            if preview:
                resource.enable_dataset_preview()
                dataset.preview_resource()

        def add_resource_with_url(url, description):
            try:
                path = self.retriever.download_file(url)
                add_resource(path, description)
                return True
            except DownloadError as ex:
                logger.exception(ex)
                return False

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
            showcase.add_tags(tags)
            showcases.append(showcase)

        success = True
        analysis_output = properties.get("analysis_output")
        if analysis_output:
            url_dict = None
            try:
                zippath = self.retriever.download_file(analysis_output)
                with ZipFile(zippath, "r") as zipfile:
                    filenamelist = zipfile.namelist()
                    order = ["json", "tiff", "gpkg", ".txt"]
                    filenames = {x[-4:]: x for x in filenamelist if x[-4:] in order}
                    sorted_extensions = sorted(filenames, key=lambda x: order.index(x))
                    for extension in sorted_extensions:
                        path = zipfile.extract(filenames[extension], path=self.folder)
                        if path.endswith("json"):
                            oldpath = path
                            path = oldpath.replace("json", "geojson")
                            rename(oldpath, path)
                            add_resource(path, "GeoJSON File", preview=True)
                        elif path.endswith("tiff"):
                            add_resource(path, "GeoTIFF File")
                        elif path.endswith("gpkg"):
                            add_resource(path, "Geopackage File")
                        else:
                            add_resource(path, "Metadata File")
            except DownloadError as ex:
                logger.exception(ex)
                success = False
            if dataset.number_of_resources() == 0:
                logger.error(f"{title} has no data files for dataset!")
                return None, None
            tags.append("geodata")
            add_showcase(
                "Report",
                "Report",
                properties["dashboard_url"],
                self.configuration["flood_image_url"],
            )
        else:
            url_dict = properties["url"]
            shape_url = url_dict.get("shapefile")
            url = url_dict.get("population_csv", url_dict.get("population"))
            if not shape_url and not url:
                logger.error(f"{title} has no data files for dataset!")
                return None, None
            if shape_url:
                success = add_resource_with_url(shape_url, "Shape File")
                tags.append("geodata")

            if url:
                success = add_resource_with_url(url, "Population Estimation")
                tags.append("affected population")
            dataset.preview_off()
        if not success:
            return None, None
        dataset.add_tags(tags)
        if not url_dict:
            return dataset, showcases

        def view_image(url_type):
            viewer_url = "https://mcarans.github.io/view-images/#"
            map_url = url_dict.get(url_type)
            if not map_url:
                return None, None
            return f"{viewer_url}{map_url}", map_url

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
        return dataset, showcases
