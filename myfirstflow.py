import requests
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash


# Basic Flow
@flow(retries=3, retry_delay_seconds=5)
def get_repo_info(url: str):
    response = requests.get(url)
    res_json = response.json()
    logger = get_run_logger()
    logger.info(f"PrefectHQ/prefect repository statistics 🤓:")
    logger.info(f"Status code: {response.status_code}")
    logger.info(f"Stars 🌠 : {res_json['stargazers_count']}")
    logger.info(f"Forks 🍴 : {res_json['forks_count']}")


# Move the request into a task
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_url(url: str, params: dict = None):
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


@flow(retries=3, retry_delay_seconds=5)
def get_repo_info_bis(repo_name: str = "PrefectHQ/prefect"):
    response = get_url(f"https://api.github.com/repos/{repo_name}")
    data = response.json()
    logger = get_run_logger()
    logger.info(f"PrefectHQ/prefect repository statistics 🤓:")
    logger.info(f"Status code: {response.status_code}")
    logger.info(f"Stars 🌠 : {data['stargazers_count']}")
    logger.info(f"Forks 🍴 : {data['forks_count']}")


if __name__ == '__main__':
    # URL = "https://api.github.com/repos/PrefectHQ/prefect"
    get_repo_info_bis()
