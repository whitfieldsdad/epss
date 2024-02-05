from epss.client import PolarsClient, Query

import polars as pl
import logging
import tempfile
import os

cfg = pl.Config()
cfg.set_tbl_rows(-1)    # Unlimited output length

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')

WORKDIR = os.path.join(tempfile.gettempdir(), 'epss')

client = PolarsClient(
    include_v1_scores=False,
    include_v2_scores=False,
    include_v3_scores=True,
)

query = Query(
    cve_ids=[
        'CVE-2019-11510',   # pre-auth arbitrary file reading from Pulse Secure SSL VPNs - CVSS 10.0
        'CVE-2020-1472',    # Microsoft Active Directory escalation of privileges - CVSS 10.0
        'CVE-2018-13379',   # pre-auth arbitrary file reading from Fortinet Fortigate SSL VPN - CVSS 9.8
        'CVE-2018-15961',   # RCE via Adobe ColdFusion (arbitrary file upload that can be used to upload a JSP web shell) - CVSS 9.8
        'CVE-2019-0604',    # RCE for Microsoft Sharepoint - CVSS 9.8
        'CVE-2019-0708',    # RCE of Windows Remote Desktop Services (RDS) - CVSS 9.8
        'CVE-2019-11580',   # Atlassian Crowd Remote Code Execution - CVSS 9.8
        'CVE-2019-19781',   # RCE of Citrix Application Delivery Controller and Citrix Gateway - CVSS 9.8
        'CVE-2020-10189',   # RCE for ZoHo ManageEngine Desktop Central - CVSS 9.8
        'CVE-2014-1812',    # Windows Local Privilege Escalation - CVSS 9.0
        'CVE-2019-3398',    # Confluence Authenticated Remote Code Execution - CVSS 8.8
        'CVE-2020-0688',    # Remote Command Execution in Microsoft Exchange - CVSS 8.8
        'CVE-2016-0167',    # local privilege escalation on older versions of Microsoft Windows - CVSS 7.8
        'CVE-2017-11774',   # RCE in Microsoft Outlook via crafted document execution (phishing) - CVSS 7.8
        'CVE-2018-8581',    # Microsoft Exchange Server escalation of privileges - CVSS 7.4
        'CVE-2019-8394',    # arbitrary pre-auth file upload to ZoHo ManageEngine ServiceDesk Plus - CVSS 6.5
    ]
)
df = client.get_scores(
    workdir=WORKDIR,
    query=query,
    drop_unchanged_scores=True,
)
print(df)
