from api.enrich_company import complete_company_enrichment

website = "https://www.orchard.ai/"
linkedin_url = "https://www.linkedin.com/company/persona-identities/"

company = complete_company_enrichment(company_website=website)
print(company)



