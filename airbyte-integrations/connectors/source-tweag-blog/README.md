# Tweag Blog Source

This is the repository for the Tweag Blog source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/tweag-blog).

## Local development

### Prerequisites

* Python (`^3.9`)
* Poetry (`^1.7`) - installation instructions [here](https://python-poetry.org/docs/#installation)



### Installing the connector

From this connector directory, run:
```bash
poetry install --with dev
```


### Create credentials

**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/tweag-blog)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `src/source_tweag_blog/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.


### Locally running the connector

```
poetry run source-tweag-blog spec
poetry run source-tweag-blog check --config secrets/config.json
poetry run source-tweag-blog discover --config secrets/config.json
poetry run source-tweag-blog read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Building the docker image

1. Install [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)
2. Run the following command to build the docker image:
```bash
airbyte-ci connectors --name=source-tweag-blog build
```

An image will be available on your host with the tag `airbyte/source-tweag-blog:dev`.


### Running as a docker container

Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-tweag-blog:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-tweag-blog:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-tweag-blog:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-tweag-blog:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

### Dependency Management

All of your dependencies should be managed via Poetry. 
To add a new dependency, run:

```bash
poetry add <package-name>
```

Please commit the changes to `pyproject.toml` and `poetry.lock` files.

## Publishing a new version of the connector

You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-tweag-blog test`
2. Bump the connector version (please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors)): 
    - bump the `dockerImageTag` value in in `metadata.yaml`
    - bump the `version` value in `pyproject.toml`
3. Make sure the `metadata.yaml` content is up to date.
4. Make sure the connector documentation and its changelog is up to date (`docs/integrations/sources/tweag-blog.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
8. Once your PR is merged, the new version of the connector will be automatically published to Docker Hub and our connector registry.