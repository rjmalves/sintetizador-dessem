from setuptools import setup, find_packages

long_description = "sintetizador_dessem"

requirements = []
with open("requirements.txt", "r") as fh:
    requirements = fh.readlines()


setup(
    name="sintetizador_dessem",
    version="1.0.0",
    author="Rogerio Alves & Mariana Noel",
    author_email="rogerioalves.ee@gmail.com",
    description="sintetizador_dessem",
    long_description=long_description,
    install_requires=requirements,
    packages=find_packages(),
    py_modules=["main", "sintetizador"],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    entry_points="""
        [console_scripts]
        sintetizador-dessem=main:main
    """,
)
