import asyncio
import os
import logging
import json
import re
import csv
import io
import tempfile
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from dotenv import load_dotenv
from api.enrich_company import complete_company_enrichment
from api.search import search_aviato_companies
from api.prospecting import prospect_companies

load_dotenv()

logger = logging.getLogger("slack_bot")


class SlackBot:
    def __init__(self):
        self.app_token = os.environ.get("SLACK_APP_TOKEN")
        self.bot_token = os.environ.get("SLACK_BOT_TOKEN")

        if not self.app_token or not self.bot_token:
            raise ValueError("SLACK_APP_TOKEN and SLACK_BOT_TOKEN must be set")

        self.web_client = AsyncWebClient(token=self.bot_token)
        self.socket_mode_client = SocketModeClient(
            app_token=self.app_token,
            web_client=self.web_client
        )
        self.socket_mode_client.socket_mode_request_listeners.append(self.handle_socket_mode_request)

        # In-memory session store for prospecting conversations
        # Keyed by (channel_id, thread_ts)
        self.prospecting_sessions = {}

    async def handle_socket_mode_request(self, client: SocketModeClient, req: SocketModeRequest):
        try:
            if req.type == "slash_commands":
                await self.handle_slash_command(client, req)
            elif req.type == "events_api":
                await self.handle_events_api(client, req)
            else:
                response = SocketModeResponse(envelope_id=req.envelope_id)
                await client.send_socket_mode_response(response)
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            response = SocketModeResponse(envelope_id=req.envelope_id)
            await client.send_socket_mode_response(response)

    async def handle_slash_command(self, client: SocketModeClient, req: SocketModeRequest):
        command = req.payload.get("command")
        text = req.payload.get("text", "").strip()
        channel_id = req.payload.get("channel_id")
        user_id = req.payload.get("user_id")

        # Acknowledge immediately
        response = SocketModeResponse(envelope_id=req.envelope_id)
        await client.send_socket_mode_response(response)

        if command == "/company":
            await self.handle_company_command(text, channel_id, user_id)
        else:
            await self.web_client.chat_postMessage(channel=channel_id, text=f"Unknown command: {command}")

    async def handle_events_api(self, client: SocketModeClient, req: SocketModeRequest):
        event = req.payload.get("event", {})
        event_type = event.get("type")

        response = SocketModeResponse(envelope_id=req.envelope_id)
        await client.send_socket_mode_response(response)

        if event_type == "message":
            await self.handle_message_event(event)
        elif event_type == "app_mention":
            await self.handle_app_mention(event)

    async def handle_message_event(self, event):
        if event.get("bot_id") or not event.get("text"):
            return

        text = event.get("text", "").strip()
        channel_id = event.get("channel")
        user_id = event.get("user")
        thread_ts = event.get("thread_ts") or event.get("ts")
        channel_type = event.get("channel_type")

        if channel_type == "im":
            # Start prospecting flow
            if text.lower().strip() == "prospecting":
                await self.handle_prospecting_start(channel_id, user_id, thread_ts)
                return

            # If user is in an active prospecting session, handle response
            if (channel_id, thread_ts) in self.prospecting_sessions:
                await self.handle_prospecting_response(text, channel_id, user_id, thread_ts)
                return

            if text.lower().startswith("company "):
                url = text[8:].strip()
                await self.handle_company_enrichment(url, channel_id, user_id, thread_ts)
            elif text.lower().startswith("search "):
                search_params = text[7:].strip()
                await self.handle_company_search(search_params, channel_id, user_id, thread_ts)

    async def handle_app_mention(self, event):
        text = event.get("text", "").strip()
        channel_id = event.get("channel")
        user_id = event.get("user")
        thread_ts = event.get("thread_ts") or event.get("ts")

        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        if text.lower() == "prospecting":
            await self.handle_prospecting_start(channel_id, user_id, thread_ts)
        elif (channel_id, thread_ts) in self.prospecting_sessions:
            await self.handle_prospecting_response(text, channel_id, user_id, thread_ts)
        elif text.lower().startswith("company "):
            url = text[8:].strip()
            await self.handle_company_command(url, channel_id, user_id, thread_ts)
        elif not text:
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text="Use `company <URL>` to enrich company data.\nExample: `company https://example.com`\nOr type `prospecting` to start a prospecting flow."
            )

    async def handle_prospecting_start(self, channel_id: str, user_id: str, thread_ts: str):
        """Initiate the prospecting conversational flow."""
        self.prospecting_sessions[(channel_id, thread_ts)] = {
            "stage": "awaiting_filters",
            "user_id": user_id,
            "filters_text": None,
            "roles": None,
        }

        examples = (
            "Let's find some contacts for you. Please provide filters on the companies or types of companies you're interested in (key:value pairs).\n"
            "The available filters are:\n"
            " - Name: nameQuery\n"
            " - Country: country\n"
            " - Region: region\n"
            " - Locality: locality\n"
            " - Industry: industryList\n"
            " - Website: website\n"
            " - LinkedIn: linkedin\n"
            " - Twitter: twitter\n"
            " - Founded: founded\n"
            " - Total Funding: totalFunding\n"
            " - Total Funding (Greater Than or Equal To): totalFunding_gte\n"
            " - Total Funding (Less Than or Equal To): totalFunding_lte\n"
            "Example usage:\n"
            "- country:United States; industryList:AI, Software; founded:2020\n"
            "- nameQuery:orchard;"
            "- industryList:Consumer, E-Commerce; founded:2010; totalFunding_gte:5000000;\n"
        )
        await self.web_client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=examples,
        )

    async def handle_prospecting_response(self, text: str, channel_id: str, user_id: str, thread_ts: str):
        """Continue the prospecting flow based on current session stage."""
        session = self.prospecting_sessions.get((channel_id, thread_ts))
        if not session:
            return

        stage = session.get("stage")
        if stage == "awaiting_filters":
            session["filters_text"] = text.strip()
            session["stage"] = "awaiting_roles"
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=(
                    "Got it. What role functions are you targeting? Provide a comma-separated list, here are the options:\n"
                    "Business development, Sales, Marketing, Engineering, Product, Design, Operations, Finance, Legal, Human Resources, Customer Support, Research.\n"
                    "If you want me to try all roles, reply `skip`."
                ),
            )
            return

        if stage == "awaiting_roles":
            roles_text = text.strip()
            roles = None
            if roles_text and roles_text.lower() != "skip":
                roles = [r.strip() for r in roles_text.split(",") if r.strip()]
            session["roles"] = roles
            session["stage"] = "running"
            await self.run_prospecting(channel_id, user_id, thread_ts, session)
            # Cleanup
            self.prospecting_sessions.pop((channel_id, thread_ts), None)
            return

    async def run_prospecting(self, channel_id: str, user_id: str, thread_ts: str, session: dict):
        """Execute the prospecting process and return a CSV file."""
        filters_text = session.get("filters_text") or ""
        roles = session.get("roles")

        await self.web_client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text="Running prospecting… this can take a couple minutes, I'll send the results in a csv file once I have them!",
        )

        try:
            # Run synchronous prospecting in a thread to avoid blocking the event loop
            result = await asyncio.to_thread(prospect_companies, filters_text, True, 100, roles)

            items = (result or {}).get("items", [])
            contacts = (result or {}).get("contacts", [])

            if not items:
                await self.web_client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text="No companies found for those filters. Try adjusting and run `prospecting` again.",
                )
                return

            # Build CSV content
            csv_content = self.create_prospecting_csv(result)

            # Write and upload CSV
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as temp_file:
                temp_file.write(csv_content)
                temp_file_path = temp_file.name

            filename = f"prospecting_results_{len(items)}_companies_{len(contacts)}_contacts.csv"
            try:
                with open(temp_file_path, "rb") as file_content:
                    await self.web_client.files_upload_v2(
                        channel=channel_id,
                        thread_ts=thread_ts,
                        file=file_content,
                        filename=filename,
                        title="Prospecting Results",
                    )
            finally:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

            # Post summary message
            contacts_count = (result or {}).get("contacts_count") or len(contacts)
            note_parts = [
                f"Found {contacts_count} contacts at {len(items)} companies" if contacts_count else "no contacts"
            ]
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=", ".join(note_parts) + ". See CSV for details.",
            )

        except Exception as e:
            logger.exception(f"Error during prospecting: {e}")
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=f"Error during prospecting: {str(e)}",
            )

    def create_prospecting_csv(self, result: dict) -> str:
        """Create CSV with company + contact info.
        If contacts are present, output one row per contact.
        Otherwise, output one row per company.
        """
        output = io.StringIO()
        writer = None

        items = (result or {}).get("items", [])
        contacts = (result or {}).get("contacts", [])
        
        # Build helper map from companyId -> {website, industryList, locality, region, country, totalFunding}
        company_map = {}
        for company in items:
            c = dict(company)
            # Normalize website like in create_csv_from_results
            if not c.get('website'):
                urls_val = c.get('URLs')
                website_candidate = None
                try:
                    if isinstance(urls_val, list) and urls_val:
                        first_item = urls_val[0]
                        if isinstance(first_item, str):
                            website_candidate = first_item
                        elif isinstance(first_item, dict):
                            website_candidate = first_item.get('website') or first_item.get('url') or first_item.get('homepage')
                    elif isinstance(urls_val, dict):
                        website_candidate = urls_val.get('website') or urls_val.get('url') or urls_val.get('homepage')
                        if not website_candidate:
                            for v in urls_val.values():
                                if isinstance(v, str) and v.startswith(('http://', 'https://')):
                                    website_candidate = v
                                    break
                    elif isinstance(urls_val, str):
                        website_candidate = urls_val
                except Exception:
                    website_candidate = None
                if website_candidate:
                    c['website'] = website_candidate
            company_map[c.get('id')] = {
                'website': c.get('website'),
                'industryList': c.get('industryList'),
                'locality': c.get('locality'),
                'region': c.get('region'),
                'country': c.get('country'),
                'totalFunding': c.get('totalFunding'),
            }

        if contacts:
            # Columns for contact-enriched export
            columns = [
                "company",
                "website",
                "industryList",
                "companyLocality",
                "companyRegion",
                "companyCountry",
                "totalFunding",
                "name",
                "title",
                "linkedin",
                "email",
                "workEmail",
                "personalEmail",
            ]
            writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for c in contacts:
                row = c.copy()
                # Fill website from company map when missing
                if not row.get('website'):
                    cm = company_map.get(row.get('companyId'))
                    if cm and cm.get('website'):
                        row['website'] = cm.get('website')
                # Normalize industry list to string for CSV
                il = row.get("industryList")
                if isinstance(il, list):
                    row["industryList"] = ", ".join([str(x) for x in il if x is not None])
                writer.writerow(row)
        else:
            # Fallback: company-only export
            columns = ["name", "website", "industryList", "locality", "region", "country", "totalFunding"]
            writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for company in items:
                row = company.copy()
                il = row.get("industryList")
                if isinstance(il, list):
                    row["industryList"] = ", ".join([str(x) for x in il if x is not None])
                writer.writerow(row)

        return output.getvalue()

    async def handle_company_command(self, text: str, channel_id: str, user_id: str, thread_ts: str = None):
        if not text:
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text="Please provide a company website or LinkedIn company URL."
            )
            return

        try:
            if "linkedin.com/company" in text.lower():
                company_data = complete_company_enrichment(company_linkedin_url=text)
            else:
                company_data = complete_company_enrichment(company_website=text)

            if not company_data:
                await self.web_client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text=f"No company data found for: {text}"
                )
                return

            blocks = self.format_company_blocks(company_data)
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                blocks=blocks,
                text=f"Company enrichment results for {company_data.get('name', 'Unknown')}"
            )

        except Exception as e:
            logger.error(f"Error processing enrichment: {e}")
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=f"Error: {str(e)}"
            )

    async def handle_company_enrichment(self, url: str, channel_id: str, user_id: str, thread_ts: str):
        if not url:
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text="Please provide a URL after 'company'. Example: company https://openai.com"
            )
            return

        try:
            match = re.search(r'<(https?://[^|>]+)\|[^>]+>', url)
            if match:
                url = match.group(1)
            elif not url.startswith(('http://', 'https://')):
                url = 'https://' + url

            if "linkedin.com/company" in url.lower():
                company_data = complete_company_enrichment(company_linkedin_url=url)
            else:
                company_data = complete_company_enrichment(company_website=url)

            if not company_data:
                await self.web_client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text=f"No company data found for: {url}"
                )
                return

            blocks = self.format_company_blocks(company_data)
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                blocks=blocks,
                text=f"Company enrichment results for {company_data.get('name', 'Unknown')}"
            )

        except Exception as e:
            logger.error(f"Error processing enrichment: {e}")
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=f"Error: {str(e)}"
            )

    def format_company_blocks(self, company_data: dict) -> list:
        """Format enriched company data into a Slack Block Kit message."""

        def format_funding(amount):
            if not amount or not isinstance(amount, (int, float)):
                return "N/A"
            if amount >= 1_000_000_000:
                return f"${amount/1_000_000_000:.1f}B"
            if amount >= 1_000_000:
                return f"${amount/1_000_000:.1f}M"
            return f"${amount:,}"

        def format_date(date_str):
            if not date_str:
                return "N/A"
            return date_str.split("-")[0]

        def format_product(product):
            """Format product to show only name and tagline."""
            name = product.get("productName", "Unnamed Product")
            tagline = product.get("tagline", "")
            return f"- *{name}*: {tagline}" if tagline else f"- *{name}*"

        name = company_data.get("name", "Unknown Company")
        legal_name = company_data.get("legalName")
        desc = company_data.get("description", "")
        founded = format_date(company_data.get("founded"))
        funding = format_funding(company_data.get("totalFunding"))
        funding_rounds = company_data.get("fundingRoundCount")
        investor_names = [i["name"] for i in company_data.get("investors", []) if i.get("name")]
        if investor_names:
            if len(investor_names) > 5:
                investors = ", ".join(investor_names[:5]) + f" … (+{len(investor_names)-5} more)"
            else:
                investors = ", ".join(investor_names)
        else:
            investors = None
        location = company_data.get("country", "")
        if company_data.get("region"):
            location += f", {company_data['region']}"
        industries = ", ".join(company_data.get("industryList", [])[:3]) or "N/A"
        founders = ", ".join([f["name"] for f in company_data.get("founders", []) if f.get("name")]) or None
        traffic = f"{company_data['currentWebTraffic']:,} visits" if company_data.get("currentWebTraffic") else None
        status = company_data.get("status")

        # Build blocks
        blocks = []

        # Header: name + legal name if available
        header_text = name
        if legal_name and legal_name != name:
            header_text = f"{name} ({legal_name})"
        blocks.append({"type": "header", "text": {"type": "plain_text", "text": header_text, "emoji": False}})

        # Core facts
        fields = []
        if founded != "N/A":
            fields.append({"type": "mrkdwn", "text": f"*Founded:*\n{founded}"})
        if funding != "N/A":
            fund_text = funding
            if funding_rounds:
                fund_text += f" ({funding_rounds} rounds)"
            fields.append({"type": "mrkdwn", "text": f"*Funding:*\n{fund_text}"})
        if investors:
            fields.append({"type": "mrkdwn", "text": f"*Investors:*\n{investors}"})
        if location:
            fields.append({"type": "mrkdwn", "text": f"*Location:*\n{location}"})
        if industries != "N/A":
            fields.append({"type": "mrkdwn", "text": f"*Industries:*\n{industries}"})
        if traffic:
            fields.append({"type": "mrkdwn", "text": f"*Web Traffic:*\n{traffic}"})
        if status:
            fields.append({"type": "mrkdwn", "text": f"*Status:*\n{status}"})

        if fields:
            blocks.append({"type": "section", "fields": fields})

        # Acquisition/exit/shutdown flags
        flags = []
        if company_data.get("isAcquired"):
            flags.append("Acquired")
        if company_data.get("isExited"):
            flags.append("Exited")
        if company_data.get("isShutDown"):
            flags.append("Shut down")
        if flags:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Flags:*\n" + ", ".join(flags)}
            })

        blocks.append({"type": "divider"})

        # Founders
        if founders:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Founders:*\n{founders}"}})

        # Description
        if desc:
            trimmed = desc if len(desc) < 600 else desc[:600].rsplit(" ", 1)[0] + "..."
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Description:*\n{trimmed}"}})

        # Optional extra sections
        def add_list_section(title, items, formatter=None, max_items=5):
            """Add a list-based section with optional custom formatting per item."""
            if not items:
                return None

            lines = []
            for item in items[:max_items]:
                if formatter:
                    lines.append(formatter(item))
                else:
                    # fallback: plain string
                    lines.append(str(item))

            if not lines:
                return None

            return {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{title}:*\n" + "\n".join(lines)}
            }

        # Website
        if company_data.get("website"):
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Website:*\n<{company_data['website']}>"}
            })

        extras = [
            add_list_section("Customer Types", company_data.get("customerTypes")),
            add_list_section("Owned Patents", company_data.get("ownedPatents")),
            add_list_section("Government Awards", company_data.get("governmentAwards")),
            add_list_section("Products", company_data.get("productList"), formatter=format_product),
            add_list_section("Business Models", company_data.get("businessModelList")),
        ]

        for section in extras:
            if section:
                blocks.append(section)


        return blocks

    async def handle_company_search(self, search_params: str, channel_id: str, user_id: str, thread_ts: str):
        """Handle company search requests in DM"""
        if not search_params:
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text='Please provide search parameters. Example: search industry: "Software, AI" country: "United States" founded: 2021'
            )
            return

        try:
            # Parse search parameters
            search_filters = self.parse_search_params(search_params)
            if not search_filters:
                await self.web_client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text='Invalid search format. Use: search industry: "Software, AI" country: "United States" founded: 2021'
                )
                return

            # Execute search
            results = search_aviato_companies(search_filters)
            companies = []
            if not results:
                companies = []
            elif isinstance(results, list):
                companies = results
            elif isinstance(results, dict):
                if "companies" in results:
                    companies = results["companies"]
                elif "items" in results:
                    companies = results["items"]
                else:
                    # fallback: if dict but no obvious key, assume it's already company list
                    companies = results


            if not companies:
                await self.web_client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text="No companies found matching your search criteria."
                )
                return

            # Truncate CSV rows for safety
            MAX_CSV_ROWS = 500
            truncated_companies = companies[:MAX_CSV_ROWS]

            # Build CSV
            csv_content = self.create_csv_from_results(truncated_companies)

            # Write to temp file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as temp_file:
                temp_file.write(csv_content)
                temp_file_path = temp_file.name

            # Prepare note for message
            total_count = len(companies)
            if total_count > MAX_CSV_ROWS:
                note = f"API returned {total_count} companies. showing top {MAX_CSV_ROWS} in csv file."
            else:
                note = f"API returned {total_count} companies."

            # Upload CSV file (no initial comment)
            filename = f"company_search_results_{len(truncated_companies)}_companies.csv"
            try:
                with open(temp_file_path, "rb") as file_content:
                    await self.web_client.files_upload_v2(
                        channel=channel_id,
                        thread_ts=thread_ts,
                        file=file_content,
                        filename=filename,
                        title="Company Search Results"
                    )
            finally:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

            # Send concise message
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=note
            )

        except Exception as e:
            # Log full traceback to aid debugging
            logger.exception(f"Error processing company search: {e}")
            await self.web_client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=f"Error processing search: {str(e)}"
            )


    def format_search_preview(self, companies: list, max_preview: int = 5) -> list:
        """Format a preview of search results for Slack (first few companies)."""
        preview = companies[:max_preview]
        lines = []
        for c in preview:
            name = c.get("name", "Unknown")
            # Coerce description to string in case API returns non-string values
            desc_raw = c.get("description")
            desc = str(desc_raw) if desc_raw is not None else ""
            desc_trimmed = (desc[:120] + "…") if len(desc) > 120 else desc
            # Coerce country to string (some payloads may provide arrays/ints)
            country_raw = c.get("country")
            country = str(country_raw) if country_raw else "N/A"
            founded = str(c.get("founded")) if c.get("founded") else "N/A"
            lines.append(f"*{name}* ({country}, {founded})\n_{desc_trimmed}_")

        text = "\n\n".join(lines)
        extra_count = len(companies) - max_preview
        if extra_count > 0:
            text += f"\n\n…and *{extra_count} more* (see CSV)."

        return [{
            "type": "section",
            "text": {"type": "mrkdwn", "text": text}
        }]



    def parse_search_params(self, params: str) -> dict:
        """Parse search parameters from text like 'industry: "Software, AI" country: "United States" founded: 2021'"""
        search_filters = {}
        params = params.replace("“", "\"").replace("”", "\"")
        
        # Pattern to match key: "value" or key: value (handles spaces in quoted values)
        pattern = r'(\w+):\s*(?:"([^"]+)"|([^\s]+))'
        matches = re.findall(pattern, params, re.IGNORECASE)
        
        for match in matches:
            key = match[0].lower()
            value = match[1] if match[1] else match[2]
            
            if key in ['industry', 'industries']:
                # Split comma-separated industries and clean up quotes
                industries = [i.strip().strip('"\'') for i in value.split(',') if i.strip()]
                search_filters['industryList'] = industries
            elif key == 'country':
                search_filters['country'] = value
            elif key == 'region':
                search_filters['region'] = value
            elif key == 'locality':
                search_filters['locality'] = value
            elif key in ['funding', 'totalfunding']:
                try:
                    search_filters['totalFunding'] = int(value)
                except ValueError:
                    continue
            elif key == 'founded':
                try:
                    search_filters['founded'] = int(value)
                except ValueError:
                    continue
        
        return search_filters

    import io

    def create_csv_from_results(self, companies: list) -> str:
        """Convert company search results to CSV format"""
        if not companies:
            return ""
        
        # Candidate CSV columns in desired order (website right after name)
        candidate_columns = ['name', 'website', 'industryList', 'locality', 'region', 'country']
        
        output = io.StringIO()
        rows = []

        # First pass: normalize fields and collect which candidate columns have any data
        non_empty_columns = set()
        for company in companies:
            processed_company = company.copy()

            # Normalize industry list to string
            if isinstance(processed_company.get('industryList'), list):
                processed_company['industryList'] = ', '.join([str(x) for x in processed_company['industryList'] if x is not None])

            # Derive website from various structures if explicit website missing
            if not processed_company.get('website'):
                urls_val = processed_company.get('URLs')
                website_candidate = None
                try:
                    if isinstance(urls_val, list) and urls_val:
                        first_item = urls_val[0]
                        if isinstance(first_item, str):
                            website_candidate = first_item
                        elif isinstance(first_item, dict):
                            website_candidate = first_item.get('website') or first_item.get('url') or first_item.get('homepage')
                    elif isinstance(urls_val, dict):
                        website_candidate = urls_val.get('website') or urls_val.get('url') or urls_val.get('homepage')
                        if not website_candidate:
                            for v in urls_val.values():
                                if isinstance(v, str) and v.startswith(('http://', 'https://')):
                                    website_candidate = v
                                    break
                    elif isinstance(urls_val, str):
                        website_candidate = urls_val
                except Exception:
                    website_candidate = None

                if website_candidate:
                    processed_company['website'] = website_candidate

            # Keep only candidate columns for the row
            row = {k: processed_company.get(k) for k in candidate_columns}

            # Determine if each column has any non-empty value
            for k, v in row.items():
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                if isinstance(v, (list, dict)) and len(v) == 0:
                    continue
                non_empty_columns.add(k)

            rows.append(row)

        # Finalize columns: only include those with data, in desired order
        columns = [c for c in candidate_columns if c in non_empty_columns]
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')

        # Write header and rows
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        
        return output.getvalue()


    async def start(self):
        logger.info("Starting Slack bot...")
        await self.socket_mode_client.connect()
        logger.info("Slack bot connected.")

    async def stop(self):
        logger.info("Stopping Slack bot...")
        await self.socket_mode_client.disconnect()
        logger.info("Slack bot stopped.")
