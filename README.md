# 165a-winter-2023
## For Formatting
Just run black . in the root directory before committing. Discuss in server any modifications to the Black configuration in project.toml before doing so. Installing black is as simple as `pip install black`
## For Testing and Coverage
Testing can be done by doing `python3 -m unittest discover tests`. For coverage, run `coverage run -m unittest discover tests` in root directory, and then `coverage report`. Try to maximize coverage, and more importantly ensure that it covers any new methods you write. Getting details over coverage vis a vis methods is done by running `coverage html; open htmlcov/index.html`.