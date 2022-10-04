import setuptools


install_requires = [
    "confluent-kafka[avro]==1.8.2",
    "mmh3",
    "prometheus_client",
    "sqlitedict",
]

dev_requires = install_requires + [
    "pip-tools",
    "pytest",
    "pytest-cov",
    "time-machine",
    "twine",
    "sphinx",
    "sphinx-autodoc-typehints",
]

packages = setuptools.find_packages()

setuptools.setup(
    name="gtfo",
    description="A simpler Kafka streams implementation in Python using confluent-kafka",
    packages=packages,
    install_requires=install_requires,
    dev_requires=dev_requires,
    include_package_data=True,
    extras_require={"dev": dev_requires},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
