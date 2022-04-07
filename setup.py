import setuptools

setuptools.setup(
    name="walmart-discounts",
    version="0.0.1",
    packages=["walmart_discounts"],
    install_requires=[
        "bs4",
        "pandas",
        "requests",
        "tqdm",
        "openpyxl"
    ]
)