#!/usr/bin/python
"""
WFP ADAM:
--------

Reads WFP ADAM data and creates datasets.

"""
import logging

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
    def __init__(self, configuration, retriever):
        self.configuration = configuration
        self.retriever = retriever
        self.last_build_date = None

    def parse_feed(self, event_type, previous_build_date):
        url = self.configuration["url"]
        event_info = self.configuration["event_types"][event_type]
        params = event_info["params"]
        url = f"{url}{event_type}?{params}"
        json = self.retriever.download_json(url)
        events = []
        for event in json["features"]:
            properties = event["properties"]
            published = parse_date(properties["published_at"])
            if published > previous_build_date:
                event["id"] = properties["event_id"]
                event["type"] = event_type
                title = event_info["title"]
                event["title"] = lazy_fstr(title, properties)
                description = event_info["description"]
                event["description"] = lazy_fstr(description, properties)
                events.append(event)
        return events

    def generate_dataset(
        self,
        event,
    ):
        """ """
        properties = event["properties"]
        title = event["title"]
        countryiso = properties["iso3"]
        countryname = Country.get_country_name_from_iso3(countryiso)
        title = f"{countryname}{title[3:]}"
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(title)
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
                "notes": event["description"],
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("1ca198b6-e490-4cd0-9c1a-5b91bad9879a")
        dataset.set_expected_update_frequency("Never")
        dataset.set_subnational(True)
        dataset.add_country_location(countryiso)
        event_type = event["type"]
        tags = [event_type]
        from_date = properties.get("from_date")
        to_date = properties.get("to_date")
        if from_date and to_date:
            dataset.set_reference_period(from_date, to_date)
        else:
            dataset.set_reference_period(properties["published_at"])

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
            dataset.add_update_resource(resource)

        shape_url = properties["url"].get("shapefile")
        url = properties["url"].get("population")
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
            map_url = properties["url"].get(url_type)
            if not map_url:
                return None, None
            return f"{viewer_url}{map_url}", map_url

        dataset.add_tags(tags)
        showcases = []

        def add_showcase(title, description, url, image_url):
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

        url, image_url = view_image("map")
        if url:
            if not shape_url:
                dataset["customviz"] = [{"url": url}]
            else:
                add_showcase("Map", "Map", url, image_url)

        url, image_url = view_image("shakemap")
        if url:
            add_showcase("Shake Map", "Shake Map", url, image_url)
        url, image_url = view_image("wind")
        if url:
            add_showcase("Wind Map", "Wind Map", url, image_url)
        url, image_url = view_image("rainfall")
        if url:
            add_showcase("Rainfall Map", "Rainfall Map", url, image_url)

        return dataset, showcases
