import requests
import os
import logging
import json 
import dotenv

dotenv.load_dotenv()

aviato_api = os.getenv("AVIATO_API_KEY")
logger = logging.getLogger(__name__)


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
    response = requests.get(
            "https://data.api.aviato.co/company/" + company_id + "/founders?perPage=100&page=1",
            headers={
                "Authorization": "Bearer " + aviato_api
            },
    )
    result = response.json()
    return result["founders"]

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



    

