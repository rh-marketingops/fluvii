import datetime
import os
from typing import Dict

from nubium_schemas import dc, NubiumDataModel


def _eloqua_hash_field(*args, **kwargs):
    kwargs["metadata"] = kwargs.get("metadata", {})
    kwargs["metadata"]["eloqua_hash"] = True
    return dc.field(*args, **kwargs)


class RichField(NubiumDataModel):
    field_value: str = ""
    last_sourced_by: str = ""
    last_modified_date: str = ""
    blank_on_purpose: str = ""

    def __setattr__(self, attribute: str, value: str) -> None:
        if not self.__dict__.get("_is_post_init_complete", False):
            super().__setattr__(attribute, value)
            return
        if attribute == "field_value":
            if self.field_value == value and value != "":
                return
            time_now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            self.__dict__["last_modified_date"] = os.environ.get("FAKE_TIME_NOW", time_now)
            self.__dict__["blank_on_purpose"] = "1" if value == "" else ""
            self.__dict__["last_sourced_by"] = os.environ.get("FLUVII_APP_NAME") or os.environ["NU_APP_NAME"]
        super().__setattr__(attribute, value)

    def __post_init__(self):
        self.__dict__["_is_post_init_complete"] = True


class Address(NubiumDataModel):
    country_name: RichField = _eloqua_hash_field(default_factory=RichField)
    country_code: RichField = dc.field(default_factory=RichField)
    address_street_1: RichField = _eloqua_hash_field(default_factory=RichField)
    address_street_2: RichField = _eloqua_hash_field(default_factory=RichField)
    address_street_3: RichField = _eloqua_hash_field(default_factory=RichField)
    address_city: RichField = _eloqua_hash_field(default_factory=RichField)
    address_state_province: RichField = _eloqua_hash_field(default_factory=RichField)
    address_postal_code: RichField = _eloqua_hash_field(default_factory=RichField)
    core_based_statistical_area: RichField = _eloqua_hash_field(default_factory=RichField)
    combined_statistical_area: RichField = _eloqua_hash_field(default_factory=RichField)


class Job(NubiumDataModel):
    company: RichField = _eloqua_hash_field(default_factory=RichField)
    business_phone: RichField = _eloqua_hash_field(default_factory=RichField)
    fax_number: RichField = _eloqua_hash_field(default_factory=RichField)
    job_title: RichField = _eloqua_hash_field(default_factory=RichField)
    department: RichField = _eloqua_hash_field(default_factory=RichField)
    job_role: RichField = _eloqua_hash_field(default_factory=RichField)
    job_level: RichField = _eloqua_hash_field(default_factory=RichField)
    job_function: RichField = _eloqua_hash_field(default_factory=RichField)
    annual_revenue: RichField = _eloqua_hash_field(default_factory=RichField)
    company_size: RichField = _eloqua_hash_field(default_factory=RichField)


class PersonalFacts(NubiumDataModel):
    class Meta(NubiumDataModel.Meta):
        alias_nested_items = {
            "address": "Address",
            "job": "Job",
        }

    email_address: RichField = _eloqua_hash_field(default_factory=RichField)
    is_bounceback: RichField = _eloqua_hash_field(default_factory=RichField)
    is_a_test_contact: RichField = dc.field(default_factory=RichField)
    salutation: RichField = _eloqua_hash_field(default_factory=RichField)
    first_name: RichField = _eloqua_hash_field(default_factory=RichField)
    middle_name: RichField = _eloqua_hash_field(default_factory=RichField)
    last_name: RichField = _eloqua_hash_field(default_factory=RichField)
    mobile_phone: RichField = _eloqua_hash_field(default_factory=RichField)
    language_preference: RichField = _eloqua_hash_field(default_factory=RichField)
    address: Address = dc.field(default_factory=Address)
    job: Job = dc.field(default_factory=Job)


class MarketingDescriptors(NubiumDataModel):
    persona: RichField = _eloqua_hash_field(default_factory=RichField)
    super_region: RichField = _eloqua_hash_field(default_factory=RichField)
    sub_region: RichField = _eloqua_hash_field(default_factory=RichField)
    provisional_account_match: RichField = _eloqua_hash_field(default_factory=RichField)
    lead_scores: Dict[str, Dict[str, RichField]] = dc.field(
        default_factory=lambda: {"domain": {"metric": RichField().asdict()}}
    )


class Privacy(NubiumDataModel):
    consent_email_marketing: str = _eloqua_hash_field(default="")
    consent_email_marketing_timestamp: str = ""
    consent_email_marketing_source: str = ""
    consent_share_to_partner: str = ""
    consent_share_to_partner_timestamp: str = ""
    consent_share_to_partner_source: str = ""
    consent_phone_marketing: str = ""
    consent_phone_marketing_timestamp: str = ""
    consent_phone_marketing_source: str = ""


class OptIn(NubiumDataModel):
    f_formdata_optin: str = ""
    f_formdata_optin_phone: str = ""
    f_formdata_sharetopartner: str = ""


class Location(NubiumDataModel):
    city_from_ip: str = ""
    state_province_from_ip: str = ""
    postal_code_from_ip: str = ""
    country_from_ip: str = _eloqua_hash_field(default="")
    country_from_dns: str = _eloqua_hash_field(default="")


class CampaignResponseMetadata(NubiumDataModel):
    ext_tactic_id: str = ""
    int_tactic_id: str = ""
    offer_id: str = ""
    offer_consumption_timestamp: str = ""
    is_lead_activity: str = ""


class LastSubmission(NubiumDataModel):
    class Meta(NubiumDataModel.Meta):
        alias_nested_items = {
            "opt_in": "OptIn",
            "location": "Location",
            "campaign_response_metadata": "CampaignResponseMetadata",
        }

    submission_date: str = ""
    submission_source: str = ""
    opt_in: OptIn = dc.field(default_factory=OptIn)
    location: Location = dc.field(default_factory=Location)
    campaign_response_metadata: CampaignResponseMetadata = dc.field(default_factory=CampaignResponseMetadata)


class Tombstone(NubiumDataModel):
    is_tombstoned: str = ""
    tombstone_timestamp: str = ""
    tombstone_source: str = ""
    delete_all_data: str = ""


class TrackingId(NubiumDataModel):
    record_status: str = ""
    record_made_inactive_date: str = ""
    created_date: str = ""
    attributes: Dict[str, str] = dc.field(default_factory=dict)
