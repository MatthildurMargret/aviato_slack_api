import logging
from typing import Dict, Any, List
import re

from api.search import search_aviato_companies
from api.enrich_company import get_founders, get_employees
from api.get_contact_info import get_contact_info

logger = logging.getLogger(__name__)

SUPPORTED_KEYS = {
    "namequery": "nameQuery",
    "country": "country",
    "region": "region",
    "locality": "locality",
    "industry": "industryList",
    "industrylist": "industryList",
    "website": "website",
    "linkedin": "linkedin",
    "twitter": "twitter",
    "totalfunding": "totalFunding",
    "totalfunding_gte": "totalFunding_gte",
    "totalfunding_lte": "totalFunding_lte",
    "founded": "founded",
}


def _coerce_value(key: str, value: str):
    key_lower = key.lower()
    v = value.strip()

    def _parse_number_like(s: str):
        """Best-effort parsing for numeric inputs that may come from Slack formatting.
        Handles:
        - Slack tel autolinks: <tel:10000000|10000000> -> 10000000
        - Currency symbols and commas: $10,000,000 -> 10000000
        - Shorthand: 10k, 10m, 1.2b -> scaled integer
        Returns int on success; raises ValueError on failure.
        """
        if not s:
            raise ValueError("empty")
        s = s.strip()
        # Extract from Slack tel format
        m = re.match(r"^<tel:(\d+)\|[^>]+>$", s)
        if m:
            s = m.group(1)
        # Remove surrounding angle link if someone pasted <123|123>
        m2 = re.match(r"^<([0-9,.$kmbKMB]+)\|[^>]+>$", s)
        if m2:
            s = m2.group(1)

        # Normalize currency formatting
        s_norm = s.replace(",", "").replace(" ", "").lstrip("$")
        lower = s_norm.lower()
        multiplier = 1
        if lower.endswith("k"):
            multiplier = 1_000
            s_norm = lower[:-1]
        elif lower.endswith("m"):
            multiplier = 1_000_000
            s_norm = lower[:-1]
        elif lower.endswith("b"):
            multiplier = 1_000_000_000
            s_norm = lower[:-1]
        # Allow decimals for k/m/b then cast to int
        if multiplier != 1:
            val = float(s_norm)
            return int(val * multiplier)
        # Plain integer string
        return int(s_norm)

    if key_lower in ("totalfunding", "totalfunding_gte", "totalfunding_lte", "founded"):
        # try to coerce integers; if it fails, leave as string
        try:
            return _parse_number_like(v)
        except Exception:
            return v

    # industries can be comma-separated list inside the value
    if key_lower in ("industry", "industrylist"):
        parts = [p.strip() for p in v.split(",") if p.strip()]
        return parts if len(parts) > 1 else (parts[0] if parts else "")

    return v


def build_filters_from_text(query_text: str) -> Dict[str, Any]:
    """
    Parse a lightweight "key:value" query string into a filter dict
    that is compatible with `search_aviato_companies()`.

    Supports both semicolon and comma as delimiters between key:value pairs.
    Use semicolons when values contain commas (e.g., multiple industries).

    Examples:
      - "country:United States; industry:Software; founded:2021"
      - "industryList:Consumer, Retail; founded:2021"  (semicolon separates pairs)
      - "country:United States, industry:Software, founded:2021"  (comma works if no commas in values)
      - "nameQuery:orchard, industry:AI,industry:Software"  (multiple industry keys)
      - "totalFunding_gte:5000000"  (funding greater than or equal to 5M)
      - "totalFunding_lte:10000000"  (funding less than or equal to 10M)
      - "totalFunding:5000000"  (defaults to lte)
    """
    filters: Dict[str, Any] = {}
    if not query_text:
        return filters

    # Determine delimiter: prefer semicolon, fall back to comma or newline
    # If semicolon is present, use it (allows commas within values)
    # If newline is present without semicolon, use newline
    # Otherwise use comma
    if ";" in query_text:
        delimiter = ";"
    elif "\n" in query_text:
        delimiter = "\n"
    else:
        delimiter = ","
    
    pairs = [p.strip() for p in query_text.split(delimiter) if p.strip()]

    for pair in pairs:
        if ":" not in pair:
            # Allow single token queries to be treated as nameQuery
            token = pair.strip()
            if token:
                filters["nameQuery"] = token
            continue

        key, raw_val = pair.split(":", 1)
        key_norm = key.strip().lower()
        mapped = SUPPORTED_KEYS.get(key_norm)
        if not mapped:
            logger.info(f"Ignoring unsupported key: {key}")
            continue

        coerced = _coerce_value(key_norm, raw_val)

        # Allow multiple entries to accumulate for industryList as AND across industries
        if mapped == "industryList":
            existing = filters.get("industryList")
            if existing is None:
                filters["industryList"] = coerced if isinstance(coerced, list) else [coerced]
            else:
                if isinstance(coerced, list):
                    filters["industryList"].extend(coerced)
                else:
                    filters["industryList"].append(coerced)
        # Handle totalFunding with operation suffix
        elif mapped in ("totalFunding_gte", "totalFunding_lte"):
            # Store as dict with operation
            operation = "gte" if mapped.endswith("_gte") else "lte"
            filters["totalFunding"] = {"value": coerced, "operation": operation}
        # Default totalFunding to lte
        elif mapped == "totalFunding":
            filters["totalFunding"] = {"value": coerced, "operation": "lte"}
        else:
            filters[mapped] = coerced

    # Deduplicate industries if present
    if isinstance(filters.get("industryList"), list):
        filters["industryList"] = [i for idx, i in enumerate(filters["industryList"]) if i and i not in filters["industryList"][0:idx]]

    return filters


def prospect_companies(query_text: str, enrich_with_people: bool = True, enrich_limit: int = 50, roles_of_interest: List[str] = None) -> Dict[str, Any]:
    """
    Convenience wrapper used by the Slack command. Accepts a lightweight text query,
    builds filters, and calls the Aviato company search. 
    
    Args:
        query_text: Text query to parse into filters
        enrich_with_people: If True, enriches each company with founders and employees
        enrich_limit: Maximum number of companies to enrich (default: 50)
        roles_of_interest: Optional list of role functions to filter by (e.g., ["Sales", "Marketing"])
    
    Returns:
        Dict with 'items' (list of companies) and 'count'
    """
    filters = build_filters_from_text(query_text)
    logger.info(f"Built filters from command text: {filters}")
    result = search_aviato_companies(filters)
    
    if not result or not result.get("items"):
        return {"items": [], "count": 0}
    
    # Enrich companies with people data
    if enrich_with_people:
        enriched_items = []
        companies = result.get("items", [])
        companies_to_enrich = companies[:enrich_limit]
        
        logger.info(f"Enriching {len(companies_to_enrich)} companies (out of {len(companies)} total) with founders and employees...")
        
        for idx, company in enumerate(companies_to_enrich):
            company_id = company.get("id")
            if not company_id:
                logger.warning(f"Company at index {idx} has no ID, skipping enrichment")
                enriched_items.append(company)
                continue
            
            # Get founders and employees (these functions handle errors internally)
            founders = get_founders(company_id)
            employees = get_employees(company_id)
            
            # Combine into people list
            people = []
            if founders:
                people.extend([{**f, "role": "founder"} for f in founders])
            if employees:
                people.extend([{**e, "role": "employee"} for e in employees])
            
            # Add people to company data
            enriched_company = {
                **company,
                "people": people,
                "founders_count": len(founders),
                "employees_count": len(employees),
                "total_people": len(people)
            }
            enriched_items.append(enriched_company)
            
            if people:
                logger.info(f"Enriched {company.get('name', 'Unknown')}: {len(people)} people ({len(founders)} founders, {len(employees)} employees)")
            else:
                logger.debug(f"No people data for {company.get('name', 'Unknown')}")
        
        # Filter to only include companies with at least one person
        filtered_items = [item for item in enriched_items if item.get("total_people", 0) > 0]
        logger.info(f"Filtered results: {len(filtered_items)} companies with people data (out of {len(enriched_items)} total)")
        
        result["items"] = filtered_items
        result["count"] = len(filtered_items)

    # Filter by roles of interest if specified
    if roles_of_interest:
        result = role_filters(result, roles_of_interest)

        # After role filtering, gather contact info and produce flattened contacts list
        contacts = []
        for company in result.get("items", []):
            company_id = company.get("id")
            company_name = company.get("name")
            for person in company.get("people", []):
                if person.get("role") != "employee":
                    continue
                person_data = person.get("person", {})
                person_id = person.get("personId") or person_data.get("id") or person.get("id")
                full_name = person_data.get("fullName") or person.get("fullName")
                current_title = person.get("currentTitle")
                linkedin = (person_data.get("URLs", {}) or {}).get("linkedin")

                contact_info = None
                if person_id:
                    contact_info = get_contact_info(person_id)

                # Flatten preferred email from contact_info
                preferred_email = None
                emails_count = 0
                work_email = None
                personal_email = None
                if isinstance(contact_info, dict):
                    emails_list = contact_info.get("emails") or []
                    emails_count = len([e for e in emails_list if e.get("email")])
                    if emails_list:
                        # Prefer work
                        work = next((e for e in emails_list if (e.get("type") or "").lower() == "work" and e.get("email")), None)
                        personal = next((e for e in emails_list if (e.get("type") or "").lower() == "personal" and e.get("email")), None)
                        any_email = next((e for e in emails_list if e.get("email")), None)
                        choice = work or personal or any_email
                        if work:
                            work_email = work.get("email")
                        if personal:
                            personal_email = personal.get("email")
                        if choice:
                            preferred_email = choice.get("email")

                contacts.append({
                    "personId": person_id,
                    "name": full_name,
                    "title": current_title,
                    "linkedin": linkedin,
                    "email": preferred_email,
                    "companyId": company_id,
                    "company": company_name,
                    "companyCountry": company.get("country"),
                    "companyRegion": company.get("region"),
                    "companyLocality": company.get("locality"),
                    "industryList": company.get("industryList", []),
                    "totalFunding": company.get("totalFunding"),
                    "contactInfo": contact_info,
                    "emails_count": emails_count,
                    "workEmail": work_email,
                    "personalEmail": personal_email,
                })

        result["contacts"] = contacts
        result["contacts_count"] = len(contacts)

        # Compute contact email metrics
        total = len(contacts)
        with_any_email = sum(1 for c in contacts if c.get("email"))
        with_work_email = sum(1 for c in contacts if c.get("workEmail"))
        with_personal_email = sum(1 for c in contacts if c.get("personalEmail"))
        metrics = {
            "total_contacts": total,
            "with_any_email": with_any_email,
            "with_work_email": with_work_email,
            "with_personal_email": with_personal_email,
            "coverage_any_pct": round((with_any_email / total * 100.0), 2) if total else 0.0,
            "coverage_work_pct": round((with_work_email / total * 100.0), 2) if total else 0.0,
            "coverage_personal_pct": round((with_personal_email / total * 100.0), 2) if total else 0.0,
        }
        result["contact_metrics"] = metrics
        logger.info(
            "Contact metrics | total=%s any=%s(%.2f%%) work=%s(%.2f%%) personal=%s(%.2f%%)",
            metrics["total_contacts"],
            metrics["with_any_email"], metrics["coverage_any_pct"],
            metrics["with_work_email"], metrics["coverage_work_pct"],
            metrics["with_personal_email"], metrics["coverage_personal_pct"],
        )

    return result


def role_filters(prospecting_result: Dict[str, Any], roles_of_interest: List[str]) -> Dict[str, Any]:
    """
    Filter companies to only include those with people matching the roles of interest.
    
    Args:
        prospecting_result: Result from prospect_companies() with enriched people data
        roles_of_interest: List of role function names to filter by (e.g., ["Sales", "Marketing"])
    
    Returns:
        Filtered result dict with only companies that have matching roles
    """
    ROLE_FUNCTIONS = {
        "Accounting": [
            "Accountant", "Staff Accountant", "Senior Accountant", "Accounting Manager", 
            "Controller", "Assistant Controller", "Accounting Director", "Chief Accounting Officer",
            "Bookkeeper", "Accounts Payable", "Accounts Receivable", "Payroll Specialist"
        ],
        "Administrative": [
            "Administrative Assistant", "Executive Assistant", "Office Manager", "Receptionist",
            "Office Administrator", "Administrative Coordinator", "Chief of Staff", "Secretary",
            "Office Coordinator", "Administrative Specialist"
        ],
        "Arts and Design": [
            "Graphic Designer", "UI Designer", "UX Designer", "Product Designer", "Creative Director",
            "Art Director", "Visual Designer", "Brand Designer", "Design Lead", "Senior Designer",
            "Junior Designer", "Illustrator", "Motion Designer", "3D Designer"
        ],
        "Business Development": [
            "Business Development Manager", "Business Development Representative", "BDR", "BD Manager",
            "VP Business Development", "Director of Business Development", "Head of Business Development",
            "Business Development Lead", "Strategic Partnerships Manager", "Partnerships Lead",
            "Alliance Manager", "Channel Manager", "Partnerships", "Partnership Manager",
            "Strategic Partnerships", "Alliances", "Channels", "Channel Partnerships",
            "Go To Market Partnerships", "GTM Partnerships", "Corporate Development", "Corp Dev",
            "Growth Partnerships", "Ecosystem Partnerships", "Business Partnerships",
            "Partner Manager", "Partner Development Manager", "Partnerships Director",
            "Head of Partnerships", "VP Partnerships", "Director Partnerships"
        ],
        "Consulting": [
            "Consultant", "Senior Consultant", "Management Consultant", "Strategy Consultant",
            "Principal Consultant", "Consulting Manager", "Partner", "Associate Consultant",
            "Advisory Consultant", "Business Consultant"
        ],
        "Engineering": [
            "Software Engineer", "Senior Software Engineer", "Staff Engineer", "Principal Engineer",
            "Engineering Manager", "Director of Engineering", "VP Engineering", "CTO",
            "Chief Technology Officer", "Lead Engineer", "Backend Engineer", "Frontend Engineer",
            "Full Stack Engineer", "DevOps Engineer", "Site Reliability Engineer", "SRE",
            "Data Engineer", "Machine Learning Engineer", "ML Engineer", "AI Engineer",
            "Infrastructure Engineer", "Platform Engineer", "Security Engineer", "QA Engineer",
            "Test Engineer", "Embedded Engineer", "Mobile Engineer", "iOS Engineer", "Android Engineer"
        ],
        "Finance": [
            "Financial Analyst", "Senior Financial Analyst", "Finance Manager", "Finance Director",
            "CFO", "Chief Financial Officer", "VP Finance", "Controller", "Treasurer",
            "FP&A Manager", "Financial Planning Manager", "Investment Analyst", "Finance Lead",
            "Head of Finance", "Financial Controller"
        ],
        "Human Resources": [
            "HR Manager", "HR Director", "CHRO", "Chief Human Resources Officer", "VP Human Resources",
            "HR Business Partner", "HRBP", "Recruiter", "Technical Recruiter", "Talent Acquisition",
            "Talent Acquisition Manager", "Head of Talent", "People Operations", "People Ops Manager",
            "HR Generalist", "HR Specialist", "Compensation Analyst", "Benefits Manager",
            "Employee Relations Manager", "HR Coordinator"
        ],
        "Information Technology": [
            "IT Manager", "IT Director", "CIO", "Chief Information Officer", "VP IT",
            "Systems Administrator", "Network Administrator", "IT Support", "Help Desk",
            "IT Specialist", "Systems Engineer", "Network Engineer", "IT Analyst",
            "Infrastructure Manager", "IT Operations Manager"
        ],
        "Legal": [
            "General Counsel", "Chief Legal Officer", "Legal Counsel", "Corporate Counsel",
            "Senior Counsel", "Staff Attorney", "Legal Director", "VP Legal", "Paralegal",
            "Legal Manager", "Compliance Manager", "Compliance Officer", "Legal Operations"
        ],
        "Marketing": [
            "Marketing Manager", "Marketing Director", "CMO", "Chief Marketing Officer", "VP Marketing",
            "Head of Marketing", "Content Marketing Manager", "Digital Marketing Manager",
            "Product Marketing Manager", "PMM", "Growth Marketing Manager", "Brand Manager",
            "Marketing Coordinator", "Marketing Specialist", "Social Media Manager", "SEO Manager",
            "Demand Generation Manager", "Performance Marketing Manager", "Marketing Operations",
            "Content Strategist", "Marketing Analyst", "Communications Manager", "PR Manager",
            "Growth Lead", "Head of Growth", "Lifecycle Marketing", "Email Marketing Manager",
            "Campaign Manager", "Field Marketing", "ABM Manager", "Event Marketing"
        ],
        "Operations": [
            "Operations Manager", "Operations Director", "COO", "Chief Operating Officer", "VP Operations",
            "Head of Operations", "Operations Coordinator", "Operations Analyst", "Operations Lead",
            "Business Operations Manager", "Revenue Operations", "RevOps", "Sales Operations",
            "Marketing Operations", "Operations Specialist", "Business Operations", "BizOps",
            "Strategy & Operations", "Strategy and Operations"
        ],
        "Product Management": [
            "Product Manager", "Senior Product Manager", "Principal Product Manager", "Group Product Manager",
            "Director of Product", "VP Product", "CPO", "Chief Product Officer", "Head of Product",
            "Associate Product Manager", "APM", "Product Lead", "Technical Product Manager",
            "Product Owner", "Product Analyst"
        ],
        "Purchasing": [
            "Purchasing Manager", "Procurement Manager", "Buyer", "Senior Buyer", "Purchasing Agent",
            "Procurement Specialist", "Supply Chain Manager", "Sourcing Manager", "Vendor Manager",
            "Procurement Director", "Chief Procurement Officer"
        ],
        "Sales": [
            "Sales Representative", "Account Executive", "AE", "Senior Account Executive",
            "Sales Manager", "Sales Director", "VP Sales", "CRO", "Chief Revenue Officer",
            "Head of Sales", "Sales Development Representative", "SDR", "Business Development Representative",
            "Inside Sales", "Outside Sales", "Enterprise Sales", "Regional Sales Manager",
            "Territory Manager", "Sales Engineer", "Solutions Engineer", "Sales Operations",
            "Account Manager", "Customer Success Manager", "CSM", "Growth Sales", "Channel Sales",
            "Partner Sales", "Alliances Sales", "Account Director", "Key Account Manager"
        ]
    }
    
    # Keyword/substring sets per function for broader matching
    ROLE_KEYWORDS = {
        "business development": [
            "business development", "bd", "partnership", "alliances", "channel", "corporate development",
            "corp dev", "ecosystem", "partner", "gtm", "go-to-market"
        ],
        "marketing": [
            "marketing", "demand gen", "demand generation", "growth", "brand", "communications", "pr",
            "seo", "content", "campaign", "field marketing", "lifecycle", "abm", "events"
        ],
        "sales": [
            "sales", "account executive", "ae", "sdr", "bdr", "account manager", "customer success",
            "csm", "solutions engineer", "sales engineer", "inside sales", "enterprise sales",
            "channel sales", "partner sales"
        ],
        "operations": [
            "operations", "revops", "revenue operations", "bizops", "business operations", "strategy & operations",
            "strategy and operations"
        ],
        # Add more functions here as needed
    }

    # Build lowercase maps for lookup
    role_functions_lower = {k.lower(): [t.lower() for t in v] for k, v in ROLE_FUNCTIONS.items()}
    role_keywords_lower = {k.lower(): [kw.lower() for kw in v] for k, v in ROLE_KEYWORDS.items()}

    # Seniority/level patterns to allow flexible matching
    SENIORITY_HINTS = [
        "head", "vp", "svp", "evp", "chief", "director", "manager", "lead", "principal",
        "sr", "senior", "junior", "associate"
    ]

    # Collect targeted title strings and keyword substrings
    target_titles = set()
    target_keywords = set()
    for role in roles_of_interest:
        key = role.lower()
        if key in role_functions_lower:
            target_titles.update(role_functions_lower[key])
        if key in role_keywords_lower:
            target_keywords.update(role_keywords_lower[key])
        if key not in role_functions_lower and key not in role_keywords_lower:
            logger.warning(f"Role function '{role}' not found in ROLE_FUNCTIONS/ROLE_KEYWORDS")

    if not target_titles and not target_keywords:
        logger.warning(f"No matching role functions found for: {roles_of_interest}")
        return prospecting_result

    logger.info(
        f"Filtering for roles: {roles_of_interest} with {len(target_titles)} exact titles and {len(target_keywords)} keyword patterns"
    )
    
    # Filter companies and retain only relevant employees (current role)
    filtered_companies = []
    companies = prospecting_result.get("items", [])

    for company in companies:
        people = company.get("people", [])

        matched_employees: List[Dict[str, Any]] = []

        for person in people:
            # Only consider employees for role matching
            if person.get("role") != "employee":
                continue

            positions = person.get("positionList", [])
            if not positions:
                continue

            # Determine current position: prefer one without endDate; fallback to first
            current_position = None
            for pos in positions:
                if not pos.get("endDate"):
                    current_position = pos
                    break
            if current_position is None:
                current_position = positions[0]

            title = (current_position or {}).get("title", "")
            title_l = (title or "").lower()

            def matches_role(t: str) -> bool:
                if not t:
                    return False
                # Exact title list
                if t in target_titles:
                    return True
                # Keyword substrings
                for kw in target_keywords:
                    if kw in t:
                        return True
                # If exact term present with seniority hints
                for base in target_keywords:
                    for s in SENIORITY_HINTS:
                        if f"{s} {base}" in t or f"{base} {s}" in t:
                            return True
                return False

            if title and matches_role(title_l):
                # Ensure we store personId and currentTitle for later export/contact lookup
                person_data = person.get("person", {})
                person_id = person_data.get("id") or person.get("id")
                enriched_person = {**person}
                if person_id:
                    enriched_person["personId"] = person_id
                if title:
                    enriched_person["currentTitle"] = title
                matched_employees.append(enriched_person)

        if matched_employees:
            # Keep company but only with matched employees and updated counts
            updated_company = {
                **company,
                "people": matched_employees,
                "founders_count": 0,  # only returning relevant employees per request
                "employees_count": len(matched_employees),
                "total_people": len(matched_employees),
            }
            filtered_companies.append(updated_company)

    logger.info(
        f"Role filter: {len(filtered_companies)} companies match roles {roles_of_interest} (out of {len(companies)} total). "
        f"(kept only employees with current titles in target set)"
    )

    return {
        "items": filtered_companies,
        "count": len(filtered_companies)
    }
