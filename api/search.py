import os
import requests
import io
import logging
import json
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
aviato_api = os.environ.get("AVIATO_API_KEY")

def search_aviato_companies(search_filters):
    """
    Sample search filter:
    {
        "filter": {
          "country": "United States",
          "industryList": ["Software"],
          "totalFunding": 5000000,
          "founded": 2021
        }
      }
    """
    logger.info(f"Starting company search with filters: {search_filters}")
    
    # Base DSL
    dsl = {
        "offset": 0,
        "limit": 10000,
        # Always sort by funding descending
        "sort": [{"totalFunding": "desc"}]
    }

    # Optional: add nameQuery if provided
    if "nameQuery" in search_filters:
        dsl["nameQuery"] = search_filters["nameQuery"]
        logger.info(f"Added nameQuery: {search_filters['nameQuery']}")

    logger.info("Applying sort: totalFunding desc")

    # Build filters dynamically
    filter_conditions = []
    if "country" in search_filters:
        filter_conditions.append({"country": {"operation": "eq", "value": search_filters["country"]}})
        logger.info(f"Added country filter: {search_filters['country']}")
    if "region" in search_filters:
        # Support both single region and multiple regions
        region_value = search_filters["region"]
        if isinstance(region_value, list):
            filter_conditions.append({"region": {"operation": "in", "value": region_value}})
        else:
            filter_conditions.append({"region": {"operation": "eq", "value": region_value}})
        logger.info(f"Added region filter: {region_value}")
    if "locality" in search_filters:
        # Support both single locality and multiple localities
        locality_value = search_filters["locality"]
        if isinstance(locality_value, list):
            filter_conditions.append({"locality": {"operation": "in", "value": locality_value}})
        else:
            filter_conditions.append({"locality": {"operation": "eq", "value": locality_value}})
        logger.info(f"Added locality filter: {locality_value}")
    if "locationIDList" in search_filters:
        filter_conditions.append({"locationIDList": {"operation": "in", "value": search_filters["locationIDList"]}})
        logger.info(f"Added locationIDList filter: {search_filters['locationIDList']}")
    if "industryList" in search_filters:
        filter_conditions.append({"industryList": {"operation": "in", "value": search_filters["industryList"]}})
        logger.info(f"Added industryList filter: {search_filters['industryList']}")
    if "website" in search_filters:
        filter_conditions.append({"website": {"operation": "eq", "value": search_filters["website"]}})
        logger.info(f"Added website filter: {search_filters['website']}")
    if "linkedin" in search_filters:
        filter_conditions.append({"linkedin": {"operation": "eq", "value": search_filters["linkedin"]}})
        logger.info(f"Added linkedin filter: {search_filters['linkedin']}")
    if "twitter" in search_filters:
        filter_conditions.append({"twitter": {"operation": "eq", "value": search_filters["twitter"]}})
        logger.info(f"Added twitter filter: {search_filters['twitter']}")
    if "totalFunding" in search_filters:
        filter_conditions.append({"totalFunding": {"operation": "lte", "value": search_filters["totalFunding"]}})
        logger.info(f"Added totalFunding filter (lte): {search_filters['totalFunding']}")
    if "founded" in search_filters:
        # Handle founded date - convert year to ISO datetime format for comparison
        founded_value = search_filters["founded"]
        if isinstance(founded_value, int):
            # If it's a year, convert to end of year datetime for "lte" comparison
            founded_value = f"{founded_value}-12-31T23:59:59Z"
        elif isinstance(founded_value, str) and len(founded_value) == 4 and founded_value.isdigit():
            # If it's a year string, convert to end of year datetime
            founded_value = f"{founded_value}-12-31T23:59:59Z"
        
        filter_conditions.append({"founded": {"operation": "gte", "value": founded_value}})
        logger.info(f"Added founded filter (gte): {founded_value}")
    
    # Wrap filters in AND structure if any exist
    if filter_conditions:
        dsl["filters"] = [{"AND": filter_conditions}]
        logger.info(f"Built {len(filter_conditions)} filter conditions")
    else:
        logger.warning("No filter conditions built from search_filters")

    payload = {"dsl": dsl}
    logger.info(f"Final DSL payload: {json.dumps(payload, indent=2)}")

    url = "https://data.api.aviato.co/company/search"

    headers = {
        "Authorization": f"Bearer {aviato_api}" if aviato_api else "None",
        "Content-Type": "application/json"
    }
    logger.info(f"Making API request to: {url}")
    logger.info(f"Headers (API key masked): {headers}")

    response = requests.post(url, headers=headers, json=payload)
    logger.info(f"API response status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        companies_count = len(data.get('items', [])) if data else 0
        logger.info(f"API returned {companies_count} companies")
        if companies_count == 0:
            logger.warning(f"No companies found. Data response length: {len(data)}. Data response keys: {data.keys()} ")
        return data
    else:
        logger.error(f"API error: Status {response.status_code} | Response: {response.text[:500]}")
        return None

def search_aviato_profiles(search_filters):
    # Base DSL
    dsl = {
        "offset": 0,
        "limit": 10
    }

    # Optional: add nameQuery if provided
    if "id" in search_filters:
        dsl["id"] = search_filters["id"]
    if "fullName" in search_filters:
        dsl["fullName"] = search_filters["fullName"]

    # Build filters dynamically
    filters = []
    if "location" in search_filters:
        filters.append({"location": {"operation": "eq", "value": search_filters["location"]}})
    if "website" in search_filters:
        filters.append({"website": {"operation": "eq", "value": search_filters["website"]}})
    if "linkedin" in search_filters:
        filters.append({"linkedin": {"operation": "eq", "value": search_filters["linkedin"]}})
    if "twitter" in search_filters:
        filters.append({"twitter": {"operation": "eq", "value": search_filters["twitter"]}})

    # Attach filters if any
    if filters:
        dsl["filters"] = filters

    payload = {"dsl": dsl}

    url = "https://data.api.aviato.co/person/search"

    headers = {
        "Authorization": f"Bearer {aviato_api}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        logger.error("Profile search error: %s | %s", response.status_code, response.text[:200])
        return None