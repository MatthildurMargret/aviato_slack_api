import requests
import os
import logging
import json 
import dotenv
import time
from threading import Lock

dotenv.load_dotenv()

aviato_api = os.getenv("AVIATO_API_KEY")
logger = logging.getLogger(__name__)

# Global rate limiter to track API calls across all functions
_last_api_call_time = 0
_api_call_lock = Lock()
_min_delay_between_calls = 2.0  # 2 seconds between ANY API calls (increased from 1s)


def _wait_for_rate_limit():
    """Global rate limiter - ensures minimum delay between ANY API calls"""
    global _last_api_call_time
    
    with _api_call_lock:
        current_time = time.time()
        time_since_last_call = current_time - _last_api_call_time
        
        if time_since_last_call < _min_delay_between_calls:
            sleep_time = _min_delay_between_calls - time_since_last_call
            time.sleep(sleep_time)
        
        _last_api_call_time = time.time()


def get_linkedin_id(company_linkedin_url):
    # Split the url by /company/ and take the last part
    linkedin_id = company_linkedin_url.split("/company/")[-1]
    # Remove any trailing / from the linkedin_id
    linkedin_id = linkedin_id.rstrip("/")
    return linkedin_id

def enrich_company(company_website=None, company_linkedin_url=None):
    """
    Enrich company data using website URL via Aviato API.
    Returns the raw company data from the API.
    """

    if company_website:
        response = requests.get(
            "https://data.api.aviato.co/company/enrich?website=" + company_website,
            headers={
                "Authorization": "Bearer " + aviato_api
            },
    )
    elif company_linkedin_url:
        linkedin_id = get_linkedin_id(company_linkedin_url)
        response = requests.get(
            "https://data.api.aviato.co/company/enrich?linkedinID=" + linkedin_id,
            headers={
                "Authorization": "Bearer " + aviato_api
            },
    )
    
    # Check if response is successful
    if response.status_code != 200:
        logger.error("Company API error for website %s: Status %s | Snippet: %s", 
                    company_website, response.status_code, response.text[:200])
        return None
    
    # Check if response has content
    if not response.text.strip():
        logger.warning("Empty company response")
        return None
    
    # Try to parse JSON
    try:
        company = response.json()
        return parse_enrich_response(company)
    except (ValueError, requests.exceptions.JSONDecodeError) as e:
        logger.error("Company JSON decode error: %s | Snippet: %s", 
                    e, response.text[:200])
        return None

def parse_enrich_response(response):
    """
    Parse the enrich company response and return a dictionary with the following keys:
    name, legalName, country, region, locality, URLs, linkedinID,industryList, description, founded, status
    totalFunding, fundingRoundCount, 
    productList, businessModelList, embeddedNews, isAcquired, isExited, isShutDown, jobListingList,
    embeddedNews, customerTypes, ownedPatents, governmentAwards, 
    monthlyWebTrafficChange, monthlyWebTrafficPercent, yearlyWebTrafficChange, yearlyWebTrafficPercent, currentWebTraffic, webTrafficSources, webViewerCountries, 
    """
    if not response:
        return None
    
    # Extract location details
    location_details = response.get('locationDetails', {})
    
    parsed_data = {
        # Basic company info
        'id': response.get('id'),
        'name': response.get('name'),
        'legalName': response.get('legalName'),
        'country': location_details.get('country', {}).get('name'),
        'region': location_details.get('region', {}).get('name'),
        'locality': location_details.get('locality', {}).get('name'),
        'URLs': response.get('URLs', []),
        'linkedinID': response.get('linkedinID'),
        'industryList': response.get('industryList', []),
        'description': response.get('description'),
        'founded': response.get('founded'),
        'status': response.get('status'),
        
        # Funding info
        'totalFunding': response.get('totalFunding'),
        'fundingRoundCount': response.get('fundingRoundCount'),
        
        # Products and business model
        'productList': response.get('productList', []),
        'businessModelList': response.get('businessModelList', []),
        'embeddedNews': response.get('embeddedNews', []),
        
        # Company status
        'isAcquired': response.get('isAcquired'),
        'isExited': response.get('isExited'),
        'isShutDown': response.get('isShutDown'),
        
        # Jobs and customers
        'jobListingList': response.get('jobListingList', []),
        'customerTypes': response.get('customerTypes', []),
        
        # Patents and awards
        'ownedPatents': response.get('ownedPatents', []),
        'governmentAwards': response.get('governmentAwards', []),
        
        # Web traffic data
        'monthlyWebTrafficChange': response.get('monthlyWebTrafficChange'),
        'monthlyWebTrafficPercent': response.get('monthlyWebTrafficPercent'),
        'yearlyWebTrafficChange': response.get('yearlyWebTrafficChange'),
        'yearlyWebTrafficPercent': response.get('yearlyWebTrafficPercent'),
        'currentWebTraffic': response.get('currentWebTraffic'),
        'webTrafficSources': response.get('webTrafficSources', []),
        'webViewerCountries': response.get('webViewerCountries', [])
    }
    
    return parsed_data

def get_acq(company_id):
    response = requests.get(
            "https://data.api.aviato.co/company/" + company_id + "/acquisitions?perPage=100&page=1",
            headers={
                "Authorization": "Bearer " + aviato_api
            },
    )
    result = response.json()
    return result["acquisitions"]
        

def get_founders(company_id):
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                # Exponential backoff: 5s, 10s, 20s
                retry_delay = 5 * (2 ** (attempt - 1))
                logger.info(f"Retrying get_founders for {company_id} after {retry_delay}s delay (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            
            # Use global rate limiter
            _wait_for_rate_limit()
            
            response = requests.get(
                    "https://data.api.aviato.co/company/" + company_id + "/founders?perPage=100&page=1",
                    headers={
                        "Authorization": "Bearer " + aviato_api
                    },
            )
            
            if response.status_code == 429:
                # Rate limited - retry with backoff
                if attempt < max_retries - 1:
                    continue
                else:
                    logger.warning(f"get_founders rate limited for company {company_id} after {max_retries} attempts")
                    return []
            
            if response.status_code != 200:
                logger.warning(f"get_founders returned status {response.status_code} for company {company_id}")
                return []
            
            result = response.json()
            return result.get("founders", [])
            
        except Exception as e:
            logger.warning(f"get_founders error for company {company_id}: {e}")
            if attempt < max_retries - 1:
                continue
            return []
    
    return []

def get_employees(company_id):
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                # Exponential backoff: 5s, 10s, 20s
                retry_delay = 5 * (2 ** (attempt - 1))
                logger.info(f"Retrying get_employees for {company_id} after {retry_delay}s delay (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            
            # Use global rate limiter
            _wait_for_rate_limit()
            
            response = requests.get(
                    "https://data.api.aviato.co/company/" + company_id + "/employees?perPage=100&page=1",
                    headers={
                        "Authorization": "Bearer " + aviato_api
                    },
            )
            
            if response.status_code == 429:
                # Rate limited - retry with backoff
                if attempt < max_retries - 1:
                    continue
                else:
                    logger.warning(f"get_employees rate limited for company {company_id} after {max_retries} attempts")
                    return []
            
            if response.status_code != 200:
                logger.warning(f"get_employees returned status {response.status_code} for company {company_id}")
                return []
            
            result = response.json()
            return result.get("employees", [])
            
        except Exception as e:
            logger.warning(f"get_employees error for company {company_id}: {e}")
            if attempt < max_retries - 1:
                continue
            return []
    
    return []

def get_investors(company_id):
    response = requests.get(
            "https://data.api.aviato.co/company/" + company_id + "/investments?perPage=100&page=1",
            headers={
                "Authorization": "Bearer " + aviato_api
            },
    )
    result = response.json()
    return result["investments"]


def complete_company_enrichment(company_website=None, company_linkedin_url=None):
    company = enrich_company(company_website=company_website, company_linkedin_url=company_linkedin_url)

    if not company:
        return None

    company["acquisitions"] = get_acq(company["id"])
    company["founders"] = get_founders(company["id"])
    company["investors"] = get_investors(company["id"])

    return company



    

