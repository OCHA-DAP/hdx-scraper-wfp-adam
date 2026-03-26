#!/usr/bin/python
"""
Unit tests for WFP ADAM.

"""

from hdx.scraper.wfp.adam.pipeline import Pipeline
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve


class TestADAM:
    def test_generate_datasets_and_showcases(
        self,
        configuration,
        fixtures,
        input_folder,
    ):
        with temp_dir(
            "test_wfp_adam", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                startdate = parse_date("2025-09-01")
                today = parse_date("2026-03-26")
                pipeline = Pipeline(configuration, retriever, today, folder)
                eventtype_info = configuration["event_types"]["SM"]
                latest_episodes = pipeline.parse_feed(startdate, eventtype_info)
                assert len(latest_episodes) == 1
                events = pipeline.parse_eventtypes_feed(latest_episodes, eventtype_info)
                assert len(events) == 1
                dataset, showcases = pipeline.generate_dataset(
                    latest_episodes, events[0]
                )

                assert dataset == {
                    "customviz": [
                        {
                            "url": "https://mcarans.github.io/view-images/#https://static.gis.wfp.org/adam_eq/events/2025/09/sm_us7000qtpj/sm_us7000qtpj.jpg"
                        }
                    ],
                    "data_update_frequency": "-1",
                    "dataset_date": "[2025-09-04T00:00:00 TO 2025-09-04T00:00:00]",
                    "dataset_preview": "no_preview",
                    "groups": [{"name": "arg"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "argentina-earthquake-sm-us7000qtpj",
                    "notes": "**ADAM ID: sm\\_us7000qtpj**  Magnitude 5.9 earthquake at 176.483 "
                    "depth occurred on Sep 04 2025 91 km WSW of San Antonio de los "
                    "Cobres, Argentina. The epicentre of the earthquake was located at "
                    "latitude -24.3833 longitude -67.2005. It impacted 675 people within "
                    "50km.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Argentina: Earthquake - 5.9M - Sep 2025",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "GeoJSON File",
                        "format": "geojson",
                        "name": "detail-us7000qtpj.geojson",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Shape File",
                        "format": "shp",
                        "name": "sm-us7000qtpj-sm-us7000qtpj-shp.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population Estimation",
                        "format": "csv",
                        "name": "sm-us7000qtpj-sm-us7000qtpj-pop-estimation.csv",
                    },
                ]

                assert showcases == [
                    {
                        "image_url": "https://static.gis.wfp.org/adam_eq/events/2025/09/sm_us7000qtpj/sm_us7000qtpj.jpg",
                        "name": "argentina-earthquake-sm-us7000qtpj-map-showcase",
                        "notes": "Map",
                        "tags": [
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                        "title": "Map",
                        "url": "https://static.gis.wfp.org/adam_eq/events/2025/09/sm_us7000qtpj/sm_us7000qtpj.jpg",
                    }
                ]

                eventtype_info = configuration["event_types"]["TS"]
                latest_episodes = pipeline.parse_feed(startdate, eventtype_info)
                assert len(latest_episodes) == 1
                events = pipeline.parse_eventtypes_feed(latest_episodes, eventtype_info)
                assert len(events) == 1
                dataset, showcases = pipeline.generate_dataset(
                    latest_episodes, events[0]
                )
                assert dataset == {
                    "data_update_frequency": "-1",
                    "dataset_date": "[2025-09-04T00:00:00 TO 2025-09-04T00:00:00]",
                    "dataset_preview": "resource_id",
                    "groups": [{"name": "mex"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "mexico-cyclone-1001204",
                    "notes": "**ADAM ID: 1001204\\_10**  Cyclone (category 1) during the period "
                    "Sep 02 2025-Sep 04 2025 in Mexico. The center of the storm was "
                    "located near latitude 24.0 longitude -113.7.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Mexico: Cyclone - Category 1 - Sep 2025",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "True",
                        "description": "Shape File",
                        "format": "shp",
                        "name": "1001204-10-adam-ts-1001204-10-shp.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population Estimation",
                        "format": "csv",
                        "name": "1001204-10-adam-ts-1001204-10-pop-estimation.csv",
                    },
                ]
                assert showcases == []
