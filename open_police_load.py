import dlt
from dlt.sources.helpers import requests

@dlt.source
def open_police_source(base_police_url = "https://data.police.uk/api"):

    def _get_data_with_retry(endpoint):
        url = f"{base_police_url}/{endpoint}"
        response = requests.get(url)
        return response.json()

# this resource takes data from forces
    @dlt.resource(write_disposition="replace")
    def forces():        
        yield from _get_data_with_retry("forces")

# this resource takes data from forces and gets force details
    @dlt.transformer(data_from=forces, write_disposition="replace")
    def force_specifics(id):
        short_id=id["id"]
        yield _get_data_with_retry(f"forces/{short_id}")

# this resource takes data from forces and gets force people
    @dlt.transformer(data_from=forces, write_disposition="replace")
    def force_people(id):
        short_id=id["id"]
        yield _get_data_with_retry(f"forces/{short_id}/people")
    
    return forces(), force_specifics, force_people

if __name__ == "__main__":

    pipeline = dlt.pipeline(
        pipeline_name="open_police_data",
        destination="bigquery",
        dataset_name="open_police_data",
    )

    data=open_police_source()
    # The response contains a list of issues
    load_info = pipeline.run(data)
    pipeline.run([load_info], table_name="_load_info")

    print(load_info)