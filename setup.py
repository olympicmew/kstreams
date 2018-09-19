import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kstreams",
    version="0.1",
    author="olympicmew",
    author_email="olympicmew@gmail.com",
    description="Create and maintain a database of k-pop streaming statistics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/olympicmew/kstreams",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha"
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
)
