import setuptools


with open("README.md") as file:
    long_description = file.read()

with open("requirements.txt") as file:
    install_requires = file.read().splitlines()

setuptools.setup(
    name="bi-utils-gismart",
    version="0.10.0",
    author="gismart",
    author_email="info@gismart.com",
    description="Utils for BI team",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gismart/bi-utils",
    packages=setuptools.find_packages(),
    python_requires=">=3.8",
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
