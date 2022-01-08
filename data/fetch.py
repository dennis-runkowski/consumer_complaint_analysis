"""Fetch Consumer Data from API

Documnentation - > https://cfpb.github.io/api/ccdb/api.html
"""
import logging
from datetime import datetime

import requests


class ConsumerAPI():
    """Fetch Data from the cfpb API

    Args:
        company (str): Company to get data for
        date_received_max (str): Max date 2022-01-05
        date_received_min (str): Start date 2019-01-05
    """
    def __init__(self, company, date_received_max, date_received_min):
        """Initialize the ConsumerAPI class"""
        self._log = self._setup_logging('consumer')
        self.company = company
        self.date_received_max = date_received_max
        self.date_received_min = date_received_min
        self._fetch = True
        self.api = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?'  # noqa

    def fetch_data(self):
        """Fetch data from API"""
        start_from = 0
        break_points = 2
        while self._fetch:
            data = self._request_data(start_from, None)
            if not data['hits']['hits']:
                self._log.info('Request Completed')
                self._fetch = False
                break
            start_from += 100
            # TODO - Fix this
            try:
                search_after = data['_meta']['break_points'][str(break_points)]
            except KeyError:
                self._log.info('Request Completed')
                self._fetch = False
                break
            break_points += 1
            search_after = f"{search_after[0]}_{search_after[1]}"
            self._log.info(f"Downloaded - {len(data['hits']['hits'])}")
            yield data

    def _request_data(self, frm, search_after):
        """Method to request date from search endpoint
        Args:
            frm (str): Next batch of documents
            search_after (str): Batch to search after

        Returns:
            json data from api
        """
        try:
            payload = {
                'company': self.company,
                'date_received_max': self.date_received_max,
                'date_received_min': self.date_received_min,
                'field': 'all',  # should make more flexible
                'size': 100,  # hard coded for now
                'sort': 'created_date_desc'
            }
            if frm:
                payload['frm'] = frm
                payload['search_after'] = search_after

            response = requests.get(
                self.api,
                params=payload
            )
            return response.json()
        except Exception as e:
            self._log.error(f'Error fetching data {e}')
            return {}

    def _setup_logging(self, name, level="INFO"):
        """
        Method to setup logger

        Args:
            name (str): Name of the logger
            level (str): log level
        Returns:
            obj logger
        """
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        timestamp = str(timestamp)
        logger_name = "{}_{}".format(
            name,
            timestamp
        )
        log = logging.getLogger(logger_name)
        log.setLevel(level)
        if level == "DEBUG":
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(name)s - %(message)s '
            )
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s '
            )
        handler = logging.StreamHandler()
        handler.setLevel(level=level)
        handler.setFormatter(formatter)

        log.addHandler(handler)
        return log
        
