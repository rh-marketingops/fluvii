python3 -m pip install --upgrade build twine &&
python3 -m build &&
python3 -m twine upload --skip-existing --non-interactive --username "{env:PYPI_USERNAME}" --password "{env:PYPI_PASSWORD}" dist/*