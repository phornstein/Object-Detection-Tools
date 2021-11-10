import setuptools

with open("README.md","r",encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="arcDetectionTools",
    version="1.2.1",
    author="Parker Hornstein",
    author_email="phornstein@esri.com",
    description="This package is a demo package of how a python toolbox can be included in your python distribution.",
    install_requires=[],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/phornstein/Object-Detection-Tools",
    packages=['arcdetect'],
    package_data={'arcdetect':['esri/toolboxes/*',  
                  'esri/arcpy/*', 'esri/help/gp/*',  
                  'esri/help/gp/toolboxes/*', 'esri/help/gp/messages/*']},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
