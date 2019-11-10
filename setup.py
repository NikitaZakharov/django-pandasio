try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

VERSION = "0.1alpha"

setup(
   name="django-pandasio",
   version=VERSION,
   description="Pandas DataFrames in Django",
   license="http://www.gnu.org/copyleft/gpl.html",
   platforms=["any"],
   packages=['pandasio', 'pandasio.db', 'pandasio.validation'],
   package_dir={'pandasio': 'pandasio'},
   install_requires=["six", "pandas", "django", "django-rest-framework"],
)
