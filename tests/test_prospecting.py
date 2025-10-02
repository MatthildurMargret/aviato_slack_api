import os
import logging

from api.prospecting import build_filters_from_text, prospect_companies


logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')


def demo_parser_cases():
    cases = [
        "country:United States, industry:Software, founded:2021, totalFunding:5000000",
        "nameQuery:orchard, industry:AI,industry:Software",
        # Use semicolon when industry value contains commas
        "industryList:AI, Fintech; nameQuery:test",
        "linkedin:https://www.linkedin.com/company/persona-identities/",
        # Single token becomes nameQuery
        "persona",
        # Newline-delimited tokens (no commas) are supported
        "country:Iceland\nindustry:Fintech\nfounded:2019",
        # Semicolon delimiter for multiple industries with commas
        "industryList:Consumer, Retail; founded:2021",
        "industryList:AI, Software, Fintech; country:United States; founded:2020",
        # Funding filters with operations
        "totalFunding_gte:5000000; founded:2020",
        "totalFunding_lte:10000000; industryList:Software",
    ]

    for i, case in enumerate(cases, 1):
        filters = build_filters_from_text(case)
        print(f"CASE {i}: {case}")
        print("-> FILTERS:", filters)
        print()


def demo_live_search_if_key(query):
    api_key = os.environ.get("AVIATO_API_KEY")
    if not api_key:
        print("AVIATO_API_KEY not set; skipping live search demo.")
        return

    print("Running live search demo...")
    # Keep it simple/small; server enforces sort & limit defaults in search function
    data = prospect_companies(query)

    if not isinstance(data, dict):
        print("Unexpected response type:", type(data))
        return

    items = data.get("items", [])
    print(f"Live search returned {len(items)} items.\n")
    
    for idx, item in enumerate(items[:3], 1):
        print(f"--- Company {idx}: {item.get('name', 'Unknown')} ---")
        print(f"  ID: {item.get('id')}")
        print(f"  Location: {item.get('locality', 'N/A')}, {item.get('region', 'N/A')}, {item.get('country', 'N/A')}")
        print(f"  Industries: {', '.join(item.get('industryList', []))}")
        print(f"  Total People: {item.get('total_people', 0)} ({item.get('founders_count', 0)} founders, {item.get('employees_count', 0)} employees)")
        
        # Show sample people
        people = item.get("people", [])
        if people:
            print(f"  Sample People:")
            for person in people[:5]:
                role = person.get("role", "unknown")
                
                # Handle different structures for founders vs employees
                if role == "founder":
                    name = person.get("fullName", "Unknown")
                    location = person.get("location", "N/A")
                    linkedin = person.get("URLs", {}).get("linkedin", "N/A")
                    print(f"    - {name} (founder)")
                    print(f"      Location: {location}")
                    print(f"      LinkedIn: {linkedin}")
                elif role == "employee":
                    # Employee data has nested person object
                    person_data = person.get("person", {})
                    name = person_data.get("fullName", "Unknown")
                    location = person_data.get("location", "N/A")
                    linkedin = person_data.get("URLs", {}).get("linkedin", "N/A")
                    
                    # Get current title from positionList
                    positions = person.get("positionList", [])
                    current_title = "N/A"
                    if positions:
                        # Get the most recent position (first in list or one without endDate)
                        for pos in positions:
                            if not pos.get("endDate"):
                                current_title = pos.get("title", "N/A")
                                break
                        if current_title == "N/A" and positions:
                            current_title = positions[0].get("title", "N/A")
                    
                    print(f"    - {name} (employee) - {current_title}")
                    print(f"      Location: {location}")
                    print(f"      LinkedIn: {linkedin}")
        else:
            print(f"  No people data available")
        print()


if __name__ == "__main__":
    # Parser-focused tests (no network)
    # demo_parser_cases()

    # Test different queries to understand API behavior
    print("=" * 60)
    print("TEST 1: With industries + funding filter + role filter")
    print("=" * 60)
    
    # Test with role filtering
    api_key = os.environ.get("AVIATO_API_KEY")
    if api_key:
        query = "industryList:Consumer, Retail; founded:2010; totalFunding_gte:10000000; country:United States"
        roles_env = os.environ.get("ROLE_FILTERS", "Business Development,Operations")
        roles = [r.strip() for r in roles_env.split(",") if r.strip()]
        print(f"Query: {query}")
        print(f"Role Filters: {roles}\n")

        result = prospect_companies(query, roles_of_interest=roles)

        items = result.get("items", [])
        print(f"\nLive search returned {len(items)} items after role filtering.\n")

        for idx, item in enumerate(items[:3], 1):
            print(f"--- Company {idx}: {item.get('name', 'Unknown')} ---")
            print(f"  Total People: {item.get('total_people', 0)} ({item.get('founders_count', 0)} founders, {item.get('employees_count', 0)} employees)")

            # People are already filtered by role_filters to relevant employees with current titles
            people = item.get("people", [])
            if people:
                print(f"  Relevant employees (current titles):")
                shown = 0
                for person in people:
                    if person.get("role") == "employee":
                        person_data = person.get("person", {})
                        name = person_data.get("fullName", "Unknown")
                        title = person.get("currentTitle", "N/A")
                        print(f"    - {name} - {title}")
                        shown += 1
                        if shown >= 5:
                            break
            print()

        # Show flattened contacts sample for CSV export validation
        contacts = result.get("contacts", [])
        print(f"Contacts total: {result.get('contacts_count', 0)}")
        for c in contacts[:5]:
            print(
                f"CONTACT: {c.get('name')} | {c.get('title')} | {c.get('company')} | "
                f"Email: {c.get('email')} | emails_count: {c.get('emails_count')} | "
                f"workEmail: {c.get('workEmail')} | personalEmail: {c.get('personalEmail')}"
            )

        # Print email coverage metrics
        metrics = result.get("contact_metrics", {})
        if metrics:
            print("\nEmail coverage metrics:")
            print(
                f"total_contacts={metrics.get('total_contacts')} | "
                f"with_any_email={metrics.get('with_any_email')} ({metrics.get('coverage_any_pct')}%) | "
                f"with_work_email={metrics.get('with_work_email')} ({metrics.get('coverage_work_pct')}%) | "
                f"with_personal_email={metrics.get('with_personal_email')} ({metrics.get('coverage_personal_pct')}%)"
            )
