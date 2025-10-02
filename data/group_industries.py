import json
from collections import defaultdict

# Define industry groupings based on common patterns and related fields
INDUSTRY_GROUPS = {
    "Technology & Software": [
        "software", "information technology", "it services", "it consulting",
        "saas", "cloud", "internet", "web", "mobile", "app", "digital",
        "tech", "data", "ai", "artificial intelligence", "machine learning",
        "blockchain", "crypto", "cybersecurity", "cyber security", "network",
        "database", "developer", "programming", "coding", "Information Services", "analytics", "computer"
    ],
    "Healthcare & Medical": [
        "health care", "medical", "hospital", "pharmaceutical", "biotechnology",
        "therapeutics", "dental", "clinic", "wellness", "fitness", "mental health",
        "diagnostic", "medical device", "biopharma", "drug", "healthcare", "health", "personal health"
    ],
    "Financial Services": [
        "financial services", "finance", "banking", "insurance", "fintech",
        "investment", "venture capital", "asset management", "wealth management",
        "lending", "payments", "credit", "mortgage", "accounting", "tax", "risk management"
    ],
    "Manufacturing & Industrial": [
        "manufacturing", "industrial", "machinery", "automotive", "electronics",
        "chemical", "metal", "textile", "equipment", "fabrication", "assembly",
        "production", "factory", "commercial", "hardware", "mining", "aerospace", "robotics", "infrastructure"
    ],
    "Retail & E-Commerce": [
        "retail", "e-commerce", "ecommerce", "wholesale", "shopping", "marketplace", "crm", "wine and spirits", "lifestyle"
        "store", "consumer goods", "fashion", "apparel", "clothing", "Consumer", "furniture", "cosmetics", "beauty"
    ],
    "Marketing & Advertising": [
        "marketing", "advertising", "brand", "digital marketing", "social media",
        "seo", "content", "creative", "agency", "public relations", "pr"
    ],
    "Real Estate & Construction": [
        "real estate", "construction", "property", "building", "architecture",
        "engineering", "civil engineering", "residential", "commercial real estate"
    ],
    "Food & Beverage": [
        "food", "beverage", "restaurant", "catering", "bakery", "brewery",
        "winery", "coffee", "organic food", "snack"
    ],
    "Education & Training": [
        "education", "training", "e-learning", "edtech", "school", "university",
        "tutoring", "coaching", "professional training", "learning"
    ],
    "Professional Services": [
        "consulting", "professional services", "business consulting", "legal", "sales", "business development",
        "law", "management consulting", "advisory", "strategy", "advice", "graphic design", "customer service", "business intelligence"
    ],
    "Transportation & Logistics": [
        "transportation", "logistics", "shipping", "freight", "delivery",
        "supply chain", "warehousing", "trucking", "aviation", "maritime"
    ],
    "Energy & Utilities": [
        "energy", "oil and gas", "renewable energy", "solar", "utilities",
        "power", "electric", "wind", "nuclear", "clean energy"
    ],
    "Media & Entertainment": [
        "media", "entertainment", "publishing", "broadcasting", "film",
        "music", "gaming", "video", "news", "sports", "events"
    ],
    "Telecommunications": [
        "telecommunications", "telecom", "wireless", "voip", "network",
        "internet service", "cable", "satellite"
    ],
    "Agriculture": [
        "agriculture", "farming", "agtech", "cultivation", "livestock",
        "dairy", "aquaculture", "forestry"
    ],
    "Non-Profit & Social": [
        "non profit", "nonprofit", "charity", "social", "civic", "philanthropy",
        "humanitarian", "community", "advocacy"
    ],
    "Hospitality & Travel": [
        "hospitality", "hotel", "travel", "tourism", "restaurant", "resort",
        "accommodation", "leisure"
    ],
    "Human Resources": [
        "human resources", "hr", "recruiting", "staffing", "talent",
        "employment", "payroll", "workforce"
    ],
    "Security": [
        "security", "physical security", "surveillance", "access control",
        "fire protection", "law enforcement"
    ],
    "Environmental Services": [
        "environmental", "waste management", "recycling", "sustainability",
        "water", "pollution", "cleantech", "green"
    ]
}


def categorize_industry(industry_name):
    """
    Categorize an industry into a major group based on keywords.
    Returns the group name or None if no match.
    """
    industry_lower = industry_name.lower()
    
    for group, keywords in INDUSTRY_GROUPS.items():
        for keyword in keywords:
            if keyword in industry_lower:
                return group
    
    return "Other"


def group_industries(input_file, output_file, min_count=5):
    """
    Read industries from input file, group them, filter by min count,
    and write to output file.
    """
    # Read the input file
    with open(input_file, 'r', encoding='utf-8') as f:
        industries = json.load(f)
    
    # Filter industries with at least min_count companies
    filtered_industries = [
        ind for ind in industries 
        if ind['doc_count'] >= min_count
    ]
    
    print(f"Filtered from {len(industries)} to {len(filtered_industries)} industries (>= {min_count} companies)")
    
    # Group industries by category
    grouped = defaultdict(lambda: {"industries": [], "total_count": 0})
    
    for industry in filtered_industries:
        category = categorize_industry(industry['key'])
        grouped[category]["industries"].append({
            "name": industry['key'],
            "count": industry['doc_count']
        })
        grouped[category]["total_count"] += industry['doc_count']
    
    # Sort industries within each group by count
    for category in grouped:
        grouped[category]["industries"].sort(key=lambda x: x['count'], reverse=True)
    
    # Convert to list and sort by total count
    result = []
    for category, data in grouped.items():
        result.append({
            "category": category,
            "total_companies": data["total_count"],
            "industry_count": len(data["industries"]),
            "industries": data["industries"]
        })
    
    result.sort(key=lambda x: x['total_companies'], reverse=True)
    
    # Write output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print(f"\nGrouped into {len(result)} categories:")
    for item in result:
        print(f"  {item['category']}: {item['industry_count']} industries, {item['total_companies']:,} companies")
    
    print(f"\nOutput written to: {output_file}")


if __name__ == "__main__":
    import os
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, "industryList.json")
    output_file = os.path.join(script_dir, "grouped_industries.json")
    
    group_industries(input_file, output_file, min_count=5)
