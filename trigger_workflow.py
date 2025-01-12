import os

import requests


def trigger_github_actions(repository_name, workflow, branch_name, inputs, token):
    repository_full_name = f"Oxford-Data-Processes/{repository_name}"
    url = f"https://api.github.com/repos/{repository_full_name}/actions/workflows/{workflow}.yml/dispatches"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    payload = {"ref": branch_name, "inputs": inputs}
    response = requests.post(url, headers=headers, json=payload)
    return response


def trigger_workflow(site_name, pickup_datetime, dropoff_datetime):
    token = os.environ["GITHUB_TOKEN"]
    branch_name = "main"
    location = "manchester"
    custom_config = "true"
    stage = "prod"
    repository_name = "greenmotion"
    workflow = f"trigger_workflow_{stage}"
    inputs = {
        "SITE_NAME": site_name,
        "LOCATION": location,
        "CUSTOM_CONFIG": custom_config,
        "PICKUP_DATETIME": pickup_datetime,
        "DROPOFF_DATETIME": dropoff_datetime,
    }

    response = trigger_github_actions(
        repository_name, workflow, branch_name, inputs, token
    )

    return response


site_name = "do_you_spain"

if __name__ == "__main__":
    response = trigger_workflow(site_name, "2024-12-01T10:00:00", "2024-12-08T12:00:00")
    print(response)
